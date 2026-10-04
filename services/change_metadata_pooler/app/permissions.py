"""BQ access checks, permission-issue helpers, and user-token IAM grants."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

from google.cloud import bigquery
from google.oauth2.credentials import Credentials


# Prefer dataset-level grants (never table-level).
ROLE_DATA_VIEWER = "roles/bigquery.dataViewer"
ROLE_DATA_EDITOR = "roles/bigquery.dataEditor"
ROLE_JOB_USER = "roles/bigquery.jobUser"

_LEGACY_ROLE = {
    ROLE_DATA_VIEWER: "READER",
    ROLE_DATA_EDITOR: "WRITER",
}


@dataclass(frozen=True)
class AccessNeed:
    project: str
    dataset: str
    sa_email: str
    required_role: str
    scope: str  # dataset | project
    reason: str

    @property
    def resource_label(self) -> str:
        if self.scope == "project":
            return self.project
        return f"{self.project}.{self.dataset}"


def pooler_sa_email(iam_db_user: str, fallback_project: str) -> str:
    """Map Cloud SQL IAM user form to full SA email."""
    u = (iam_db_user or "").strip()
    if u.endswith(".gserviceaccount.com"):
        return u
    if u.endswith(".iam"):
        return f"{u}.gserviceaccount.com"
    if "@" in u:
        return u
    return f"{u}@{fallback_project}.iam.gserviceaccount.com"


def classify_bq_error(
    exc: BaseException,
    *,
    project: str,
    dataset: str,
    sa_email: str,
    for_change_history: bool = False,
) -> List[AccessNeed]:
    msg = str(exc)
    low = msg.lower()
    needs: List[AccessNeed] = []

    is_perm = any(
        x in low
        for x in (
            "access denied",
            "permission",
            "forbidden",
            "403",
            "does not have",
            "not authorized",
        )
    )
    if not is_perm:
        return needs

    if for_change_history or "alter" in low or "enable_change_history" in low:
        needs.append(
            AccessNeed(
                project=project,
                dataset=dataset,
                sa_email=sa_email,
                required_role=ROLE_DATA_EDITOR,
                scope="dataset",
                reason="enable change history / write table metadata",
            )
        )
    else:
        needs.append(
            AccessNeed(
                project=project,
                dataset=dataset,
                sa_email=sa_email,
                required_role=ROLE_DATA_VIEWER,
                scope="dataset",
                reason="read tables / INFORMATION_SCHEMA / CHANGES",
            )
        )

    # Avoid matching incidental "Job ID: …" suffixes in BQ error text.
    if any(
        x in low
        for x in (
            "bigquery.jobs.create",
            "permission bigquery.jobs",
            "does not have bigquery.jobs",
            "lacking permission bigquery.jobs",
        )
    ):
        needs.append(
            AccessNeed(
                project=project,
                dataset=dataset,
                sa_email=sa_email,
                required_role=ROLE_JOB_USER,
                scope="project",
                reason="run BigQuery jobs in this project",
            )
        )
    return needs


def gcloud_grant_command(need: AccessNeed) -> str:
    """Shell recipe shown in the UI.

    Dataset IAM via ``gcloud beta bq datasets …`` is not a real command, and
    ``bq add-iam-policy-binding`` on datasets often requires allowlisting.
    Prefer classic dataset ACL (same as Grant & retry): READER/WRITER.
    """
    member = f"serviceAccount:{need.sa_email}"
    if need.scope == "project" or need.required_role == ROLE_JOB_USER:
        return (
            f"gcloud projects add-iam-policy-binding {need.project} \\\n"
            f"  --member='{member}' \\\n"
            f"  --role={need.required_role}"
        )
    legacy = _LEGACY_ROLE.get(need.required_role, "READER")
    # jq + bq update — works without dataset-IAM allowlist
    return (
        f"bq show --format=prettyjson {need.project}:{need.dataset} \\\n"
        f"  | jq --arg m '{need.sa_email}' --arg r '{legacy}' \\\n"
        f"    '.access |= (map(select(.userByEmail != $m or .role != $r))"
        f" + [{{role: $r, userByEmail: $m}}])' \\\n"
        f"  > /tmp/bq-ds-acl.json \\\n"
        f"&& bq update --source /tmp/bq-ds-acl.json {need.project}:{need.dataset}"
    )



def need_to_dict(need: AccessNeed) -> Dict[str, Any]:
    return {
        "project": need.project,
        "dataset": need.dataset,
        "sa_email": need.sa_email,
        "required_role": need.required_role,
        "scope": need.scope,
        "reason": need.reason,
        "resource": need.resource_label,
        "gcloud": gcloud_grant_command(need),
    }


def list_dataset_tables(client: bigquery.Client, project: str, dataset: str) -> List[str]:
    sql = f"""
        SELECT table_name
        FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
        WHERE table_type = 'BASE TABLE'
        ORDER BY table_name
    """
    rows = list(client.query(sql).result())
    return [str(r["table_name"]) for r in rows]


def probe_table_access(
    client: bigquery.Client, project: str, dataset: str, table: str
) -> None:
    """Raise if the client cannot read table metadata."""
    client.get_table(f"{project}.{dataset}.{table}")


def probe_access_need(client: bigquery.Client, need: AccessNeed) -> None:
    """Raise if the pooler client still lacks the granted capability."""
    if need.required_role == ROLE_JOB_USER or need.scope == "project":
        job_client = bigquery.Client(
            project=need.project,
            credentials=client._credentials,  # noqa: SLF001 — ADC from same SA
            location=getattr(client, "location", None),
        )
        list(job_client.query("SELECT 1").result())
        return
    # Dataset-level: INFORMATION_SCHEMA is enough for viewer; get_dataset for editor.
    if need.required_role == ROLE_DATA_EDITOR:
        client.get_dataset(f"{need.project}.{need.dataset}")
        return
    list_dataset_tables(client, need.project, need.dataset)


def grant_with_user_token(access_token: str, need: AccessNeed) -> None:
    """Grant dataset/project access using the caller's OAuth access token.

    Dataset grants use BigQuery dataset ACL (READER/WRITER) — never table-level.
    Project grants use Cloud Resource Manager IAM.
    """
    creds = Credentials(token=access_token)
    member_sa = need.sa_email

    if need.scope == "project" or need.required_role == ROLE_JOB_USER:
        from googleapiclient.discovery import build

        crm = build(
            "cloudresourcemanager", "v1", credentials=creds, cache_discovery=False
        )
        policy = crm.projects().getIamPolicy(resource=need.project, body={}).execute()
        member = f"serviceAccount:{member_sa}"
        bindings = policy.setdefault("bindings", [])
        for b in bindings:
            if b.get("role") == need.required_role:
                members = b.setdefault("members", [])
                if member not in members:
                    members.append(member)
                break
        else:
            bindings.append({"role": need.required_role, "members": [member]})
        crm.projects().setIamPolicy(
            resource=need.project, body={"policy": policy}
        ).execute()
        return

    legacy = _LEGACY_ROLE.get(need.required_role)
    if not legacy:
        raise ValueError(f"unsupported dataset role {need.required_role}")

    bq = bigquery.Client(credentials=creds, project=need.project)
    ds = bq.get_dataset(f"{need.project}.{need.dataset}")
    entries = list(ds.access_entries)
    for e in entries:
        if (
            getattr(e, "entity_id", None) == member_sa
            and getattr(e, "role", None) == legacy
        ):
            return  # already granted
    entries.append(
        bigquery.AccessEntry(
            role=legacy, entity_type="userByEmail", entity_id=member_sa
        )
    )
    ds.access_entries = entries
    bq.update_dataset(ds, ["access_entries"])


def enrich_issue(issue: Dict[str, Any]) -> Dict[str, Any]:
    need = AccessNeed(
        project=issue["project"],
        dataset=issue["dataset"],
        sa_email=issue["sa_email"],
        required_role=issue["required_role"],
        scope=issue.get("scope") or "dataset",
        reason=issue.get("last_error") or "",
    )
    out = dict(issue)
    out["gcloud"] = gcloud_grant_command(need)
    out["resource"] = need.resource_label
    return out


def parse_target(raw: str) -> Tuple[str, str, Optional[str]]:
    """Parse ``project.dataset`` or ``project.dataset.table`` (backticks optional)."""
    s = raw.strip().replace("`", "")
    parts = [p for p in s.split(".") if p]
    if len(parts) == 2:
        return parts[0], parts[1], None
    if len(parts) == 3:
        return parts[0], parts[1], parts[2]
    raise ValueError("expected project.dataset or project.dataset.table")
