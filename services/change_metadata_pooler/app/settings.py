"""Environment configuration for the pooler service."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # Cloud SQL
    instance_connection_name: str
    database: str
    schema_name: str
    ip_type: str
    iam_user: str

    # BigQuery job project (billing / execution)
    bq_project: str
    bq_location: str

    # Pooler defaults
    worker_pool_size: int
    schedule_group: str

    # Full runtime SA email (for IAM grant messaging)
    pooler_sa_email: str
    # Google OAuth web client id for "Grant & retry" (GIS token client)
    oauth_client_id: str

    @classmethod
    def from_env(cls) -> "Settings":
        instance = os.environ.get("INSTANCE_CONNECTION_NAME", "").strip()
        if not instance:
            raise RuntimeError("INSTANCE_CONNECTION_NAME is required")
        iam_user = os.environ.get("CLOUDSQL_IAM_USER", "").strip()
        if not iam_user:
            raise RuntimeError(
                "CLOUDSQL_IAM_USER is required "
                "(Cloud SQL IAM DB user, e.g. pooler@my-gcp-project.iam)"
            )
        bq_project = os.environ.get("BQ_PROJECT", "").strip()
        if not bq_project:
            # default: project part of instance connection name
            bq_project = instance.split(":", 1)[0]
        sa = os.environ.get("POOLER_SA_EMAIL", "").strip()
        if not sa:
            if iam_user.endswith(".iam"):
                sa = f"{iam_user}.gserviceaccount.com"
            elif iam_user.endswith(".gserviceaccount.com"):
                sa = iam_user
            else:
                sa = f"change-metadata-pooler@{bq_project}.iam.gserviceaccount.com"
        return cls(
            instance_connection_name=instance,
            database=os.environ.get("CLOUDSQL_DATABASE", "metadata"),
            schema_name=os.environ.get("CLOUDSQL_SCHEMA", "public"),
            ip_type=os.environ.get("CLOUDSQL_IP_TYPE", "private").lower(),
            iam_user=iam_user,
            bq_project=bq_project,
            bq_location=os.environ.get("BQ_LOCATION", "US"),
            worker_pool_size=int(os.environ.get("WORKER_POOL_SIZE", "8")),
            schedule_group=os.environ.get("SCHEDULE_GROUP", "default"),
            pooler_sa_email=sa,
            oauth_client_id=os.environ.get("OAUTH_CLIENT_ID", "").strip(),
        )
