"""FastAPI entrypoint for the change-metadata pooler service."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse, JSONResponse
from pydantic import BaseModel, Field

from app.deps import get_bq_client, get_gateway, get_pooler, get_settings
from app import permissions as perms
from app.run_session import PoolRunSession
from app.ui import (
    render_index,
    render_permissions,
    render_register,
    render_run_detail,
    render_runs,
    render_table_detail,
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("change_metadata_pooler")

app = FastAPI(title="change-metadata-pooler", version="0.1.0")


def _iap_user(request: Request) -> Optional[str]:
    """Email from IAP (or empty when using Cloud Run IAM / local)."""
    raw = request.headers.get("X-Goog-Authenticated-User-Email") or ""
    # accounts.google.com:user@domain
    if ":" in raw:
        return raw.split(":", 1)[1]
    return raw or None


def _persist_needs(needs: List[perms.AccessNeed], error: str) -> List[Dict[str, Any]]:
    gw = get_gateway()
    out: List[Dict[str, Any]] = []
    for need in needs:
        try:
            issue_id = gw.upsert_permission_issue(
                project=need.project,
                dataset=need.dataset,
                sa_email=need.sa_email,
                required_role=need.required_role,
                scope=need.scope,
                last_error=(error or need.reason)[:2000],
            )
        except Exception:
            logger.exception("failed to persist permission issue")
            issue_id = None
        d = perms.need_to_dict(need)
        if issue_id is not None:
            d["id"] = issue_id
        out.append(d)
    return out


def _permission_denied(message: str, needs: List[Dict[str, Any]]) -> JSONResponse:
    return JSONResponse(
        status_code=403,
        content={"detail": message, "message": message, "needs": needs},
    )


def _preview_target(target: str) -> Dict[str, Any]:
    """Return preview payload or raise HTTPException / return via ``_permission_denied``."""
    settings = get_settings()
    try:
        project, dataset, table = perms.parse_target(target)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    client = get_bq_client()
    sa = settings.pooler_sa_email
    try:
        if table:
            perms.probe_table_access(client, project, dataset, table)
            tables = [table]
        else:
            tables = perms.list_dataset_tables(client, project, dataset)
    except Exception as exc:
        needs = perms.classify_bq_error(
            exc, project=project, dataset=dataset, sa_email=sa
        )
        if not needs:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        persisted = _persist_needs(needs, str(exc))
        label = f"{project}.{dataset}" + (f".{table}" if table else "")
        return {
            "_denied": True,
            "detail": f"Pooler SA missing access for {label}: {exc}",
            "message": f"Pooler SA missing access for {label}",
            "needs": persisted,
            "project": project,
            "dataset": dataset,
            "table": table,
            "tables": [],
            "sa_email": sa,
        }
    return {
        "_denied": False,
        "project": project,
        "dataset": dataset,
        "table": table,
        "tables": tables,
        "sa_email": sa,
        "needs": [],
    }


class RelationBody(BaseModel):
    project: Optional[str] = None
    dataset: Optional[str] = None
    table: Optional[str] = None
    # adapter-compatible aliases
    database: Optional[str] = None
    schema_name: Optional[str] = Field(default=None, alias="schema")
    identifier: Optional[str] = None

    def as_relation(self) -> Dict[str, str]:
        project = self.project or self.database
        dataset = self.dataset or self.schema_name
        table = self.table or self.identifier
        if not project or not dataset or not table:
            raise ValueError("project/dataset/table (or database/schema/identifier) required")
        return {
            "database": project,
            "schema": dataset,
            "identifier": table,
        }


class RegisterBody(BaseModel):
    project: str
    dataset: str
    table: str
    schedule_group: str = "default"


class RegisterTargetBody(BaseModel):
    target: str
    tables: Optional[List[str]] = None
    schedule_group: str = "default"


class GrantBody(BaseModel):
    access_token: str
    project: str
    dataset: str
    sa_email: str
    required_role: str
    scope: str = "dataset"
    issue_id: Optional[int] = None
    retry_target: Optional[str] = None


class PoolBody(BaseModel):
    relations: Optional[List[RelationBody]] = None
    end_ts: Optional[str] = None
    worker_pool_size: int = 0
    invocation_id: Optional[str] = None
    schedule_group: Optional[str] = None


@app.get("/health")
@app.get("/healthz")
def health() -> Dict[str, str]:
    return {"status": "ok"}


@app.get("/", response_class=HTMLResponse)
def ui_index(
    request: Request,
    enabled_only: bool = Query(default=False),
    q: Optional[str] = Query(default=None),
) -> HTMLResponse:
    gw = get_gateway()
    tables = gw.list_change_tracking_registry(enabled_only=enabled_only)
    if q:
        needle = q.lower()
        tables = [
            t
            for t in tables
            if needle in (t.get("full_table_name") or "").lower()
            or needle in (t.get("table") or "").lower()
            or needle in (t.get("dataset") or "").lower()
        ]
    return HTMLResponse(
        render_index(tables, user_email=_iap_user(request), enabled_only=enabled_only)
    )


@app.get("/ui/tables/{project}/{dataset}/{table}", response_class=HTMLResponse)
def ui_table_detail(
    request: Request,
    project: str,
    dataset: str,
    table: str,
    limit: int = Query(default=50, ge=1, le=500),
) -> HTMLResponse:
    gw = get_gateway()
    reg = gw.get_change_tracking_registry_row(project, dataset, table)
    logs = gw.list_change_tracking_log(project, dataset, table, limit=limit)
    cp = gw.get_pooler_checkpoint(project, dataset, table)
    watermark = None
    if cp and cp.get("delta_end_time") is not None:
        watermark = str(cp.get("delta_end_time"))
    return HTMLResponse(
        render_table_detail(
            reg,
            logs,
            project=project,
            dataset=dataset,
            table=table,
            watermark=watermark,
            user_email=_iap_user(request),
        )
    )


@app.get("/ui/runs", response_class=HTMLResponse)
def ui_runs(
    request: Request,
    limit: int = Query(default=100, ge=1, le=500),
) -> HTMLResponse:
    gw = get_gateway()
    runs = gw.list_pooler_runs(limit=limit)
    return HTMLResponse(render_runs(runs, user_email=_iap_user(request)))


@app.get("/ui/runs/{run_id}", response_class=HTMLResponse)
def ui_run_detail(request: Request, run_id: int) -> HTMLResponse:
    gw = get_gateway()
    run = gw.get_pooler_run(run_id)
    events = gw.list_pooler_run_events(run_id) if run else []
    return HTMLResponse(
        render_run_detail(run, events, user_email=_iap_user(request))
    )


@app.get("/ui/register", response_class=HTMLResponse)
def ui_register(request: Request) -> HTMLResponse:
    settings = get_settings()
    return HTMLResponse(
        render_register(
            user_email=_iap_user(request),
            oauth_client_id=settings.oauth_client_id,
            sa_email=settings.pooler_sa_email,
        )
    )


@app.get("/ui/permissions", response_class=HTMLResponse)
def ui_permissions(request: Request) -> HTMLResponse:
    settings = get_settings()
    issues = [
        perms.enrich_issue(i)
        for i in get_gateway().list_permission_issues(status="open", limit=200)
    ]
    return HTMLResponse(
        render_permissions(
            issues,
            user_email=_iap_user(request),
            oauth_client_id=settings.oauth_client_id,
        )
    )


@app.get("/v1/tables/{project}/{dataset}/{table}")
def get_table(
    project: str,
    dataset: str,
    table: str,
    limit: int = Query(default=50, ge=1, le=500),
) -> Dict[str, Any]:
    gw = get_gateway()
    reg = gw.get_change_tracking_registry_row(project, dataset, table)
    if reg is None:
        raise HTTPException(status_code=404, detail="table not registered")
    logs = gw.list_change_tracking_log(project, dataset, table, limit=limit)
    cp = gw.get_pooler_checkpoint(project, dataset, table)
    return {
        "registry": reg,
        "checkpoint": cp,
        "logs": logs,
        "count": len(logs),
    }


@app.post("/v1/tables/register")
def register_table(body: RegisterBody) -> Dict[str, Any]:
    gw = get_gateway()
    return gw.register_change_tracking_table(
        body.project,
        body.dataset,
        body.table,
        schedule_group=body.schedule_group,
    )


@app.post("/v1/register/preview")
def register_preview(body: RegisterTargetBody) -> Any:
    result = _preview_target(body.target)
    if result.pop("_denied", False):
        return _permission_denied(result.get("message") or result["detail"], result["needs"])
    return result


@app.post("/v1/register")
def register_target(body: RegisterTargetBody) -> Any:
    preview = _preview_target(body.target)
    if preview.pop("_denied", False):
        return _permission_denied(
            preview.get("message") or preview["detail"], preview["needs"]
        )
    project = preview["project"]
    dataset = preview["dataset"]
    tables = body.tables if body.tables is not None else preview["tables"]
    if preview.get("table") and not body.tables:
        tables = [preview["table"]]
    if not tables:
        raise HTTPException(status_code=400, detail="no tables to register")

    gw = get_gateway()
    settings = get_settings()
    client = get_bq_client()
    registered = 0
    failed = 0
    errors: List[Dict[str, Any]] = []
    all_needs: List[Dict[str, Any]] = []
    for table in tables:
        try:
            perms.probe_table_access(client, project, dataset, table)
            gw.register_change_tracking_table(
                project, dataset, table, schedule_group=body.schedule_group
            )
            registered += 1
        except Exception as exc:
            failed += 1
            needs = perms.classify_bq_error(
                exc,
                project=project,
                dataset=dataset,
                sa_email=settings.pooler_sa_email,
            )
            persisted = _persist_needs(needs, str(exc)) if needs else []
            all_needs.extend(persisted)
            errors.append({"table": table, "error": str(exc), "needs": persisted})

    status = 200 if failed == 0 else (403 if all_needs and registered == 0 else 207)
    payload = {
        "project": project,
        "dataset": dataset,
        "registered": registered,
        "failed": failed,
        "errors": errors,
        "needs": all_needs,
    }
    if status == 200:
        return payload
    return JSONResponse(status_code=status, content=payload)


@app.post("/v1/tables/unregister")
def unregister_table(body: RegisterBody) -> Dict[str, Any]:
    gw = get_gateway()
    return gw.unregister_change_tracking_table(body.project, body.dataset, body.table)


@app.get("/v1/permissions")
def list_permissions(
    status: str = Query(default="open"),
    limit: int = Query(default=200, ge=1, le=500),
) -> Dict[str, Any]:
    issues = [
        perms.enrich_issue(i)
        for i in get_gateway().list_permission_issues(status=status, limit=limit)
    ]
    return {"issues": issues, "count": len(issues)}


@app.post("/v1/permissions/grant")
def grant_permission(body: GrantBody) -> Any:
    need = perms.AccessNeed(
        project=body.project,
        dataset=body.dataset,
        sa_email=body.sa_email,
        required_role=body.required_role,
        scope=body.scope,
        reason="",
    )
    try:
        perms.grant_with_user_token(body.access_token, need)
    except Exception as exc:
        logger.warning("grant failed: %s", exc)
        raise HTTPException(
            status_code=400,
            detail=(
                f"Grant failed (does your user have permission to update "
                f"{'project IAM' if need.scope == 'project' else 'dataset ACL'}?): {exc}"
            ),
        ) from exc

    client = get_bq_client()
    resolved = False
    retry_payload: Optional[Dict[str, Any]] = None
    probe_error: Optional[str] = None
    try:
        perms.probe_access_need(client, need)
        resolved = True
    except Exception as exc:
        probe_error = str(exc)

    if body.retry_target:
        retry_payload = _preview_target(body.retry_target)
        denied = retry_payload.pop("_denied", False)
        if not denied:
            resolved = True
            retry_payload["needs"] = []
        else:
            return JSONResponse(
                status_code=403,
                content={
                    "detail": retry_payload.get("message") or retry_payload["detail"],
                    "message": "Granted, but access probe still failing",
                    "granted": True,
                    "resolved": False,
                    "needs": retry_payload.get("needs") or [],
                    "tables": retry_payload.get("tables") or [],
                    "project": retry_payload.get("project"),
                    "dataset": retry_payload.get("dataset"),
                    "table": retry_payload.get("table"),
                },
            )

    if resolved and body.issue_id is not None:
        try:
            get_gateway().resolve_permission_issue(body.issue_id)
        except Exception:
            logger.exception("failed to mark issue %s resolved", body.issue_id)

    if resolved:
        out: Dict[str, Any] = {
            "granted": True,
            "resolved": True,
            "message": f"Granted {need.required_role} on {need.resource_label}",
            "needs": [],
        }
        if retry_payload:
            out["tables"] = retry_payload.get("tables") or []
            out["project"] = retry_payload.get("project")
            out["dataset"] = retry_payload.get("dataset")
            out["table"] = retry_payload.get("table")
        return out

    return JSONResponse(
        status_code=403,
        content={
            "detail": probe_error or "Access still denied after grant",
            "message": "Granted, but pooler SA probe still failing (propagation delay?)",
            "granted": True,
            "resolved": False,
            "needs": [perms.need_to_dict(need)],
        },
    )


@app.get("/v1/tables")
def list_tables(
    enabled_only: bool = Query(default=False),
    schedule_group: Optional[str] = Query(default=None),
) -> Dict[str, Any]:
    gw = get_gateway()
    rows = gw.list_change_tracking_registry(
        enabled_only=enabled_only,
        schedule_group=schedule_group,
    )
    return {"tables": rows, "count": len(rows)}


@app.post("/v1/pool")
def pool(body: PoolBody) -> Dict[str, Any]:
    settings = get_settings()
    pooler = get_pooler()
    gw = get_gateway()

    relations: List[Dict[str, str]] = []
    group = body.schedule_group or settings.schedule_group
    if body.relations:
        try:
            relations = [r.as_relation() for r in body.relations]
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
        trigger = "api"
    else:
        registered = gw.list_change_tracking_registry(
            enabled_only=True, schedule_group=group
        )
        relations = [
            {
                "database": r["project"],
                "schema": r["dataset"],
                "identifier": r["table"],
            }
            for r in registered
        ]
        trigger = "api"

    session = PoolRunSession(
        gw,
        trigger=trigger,
        schedule_group=group,
        invocation_id=body.invocation_id,
        watermark_to=body.end_ts,
        num_tables=len(relations),
    )
    results = pooler.pool_relations(
        relations,
        worker_pool_size=body.worker_pool_size or settings.worker_pool_size,
        end_ts=body.end_ts,
        invocation_id=body.invocation_id,
        on_event=session.on_event,
    )
    run_meta = session.finish(results)
    return {
        "results": results,
        "count": len(results),
        "run": run_meta,
    }


@app.post("/v1/pool/scheduled")
def pool_scheduled(body: Optional[PoolBody] = None) -> Dict[str, Any]:
    """Cloud Scheduler target: pool all enabled registry rows."""
    settings = get_settings()
    pooler = get_pooler()
    gw = get_gateway()
    payload = body or PoolBody()
    group = payload.schedule_group or settings.schedule_group
    registered = gw.list_change_tracking_registry(
        enabled_only=True, schedule_group=group
    )
    relations = [
        {
            "database": r["project"],
            "schema": r["dataset"],
            "identifier": r["table"],
        }
        for r in registered
    ]
    session = PoolRunSession(
        gw,
        trigger="scheduled",
        schedule_group=group,
        invocation_id=payload.invocation_id,
        watermark_to=payload.end_ts,
        num_tables=len(relations),
    )
    results = pooler.pool_relations(
        relations,
        worker_pool_size=payload.worker_pool_size or settings.worker_pool_size,
        end_ts=payload.end_ts,
        invocation_id=payload.invocation_id,
        on_event=session.on_event,
    )
    run_meta = session.finish(results)
    return {
        "results": results,
        "count": len(results),
        "run": run_meta,
    }


@app.get("/v1/runs")
def list_runs(limit: int = Query(default=100, ge=1, le=500)) -> Dict[str, Any]:
    runs = get_gateway().list_pooler_runs(limit=limit)
    return {"runs": runs, "count": len(runs)}


@app.get("/v1/runs/{run_id}")
def get_run(run_id: int) -> Dict[str, Any]:
    gw = get_gateway()
    run = gw.get_pooler_run(run_id)
    if run is None:
        raise HTTPException(status_code=404, detail="run not found")
    events = gw.list_pooler_run_events(run_id)
    return {"run": run, "events": events, "count": len(events)}


@app.get("/v1/partitions")
def partitions(
    project: str = Query(...),
    dataset: str = Query(...),
    table: str = Query(...),
    start: str = Query(..., description="Interval start (inclusive)"),
    end: str = Query(..., description="Interval end (exclusive)"),
    ensure_fresh: bool = Query(default=True),
) -> Dict[str, Any]:
    pooler = get_pooler()
    return pooler.ensure_affected_partitions(
        project,
        dataset,
        table,
        start,
        end,
        ensure_fresh=ensure_fresh,
    )
