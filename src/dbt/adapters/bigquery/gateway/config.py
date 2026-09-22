from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Mapping, Optional

from dbt_common.exceptions import DbtConfigError, DbtRuntimeError


DEFAULT_EPOCH = "1900-01-01 00:00:00.000000"
DEFAULT_DATABASE = "metadata"
DEFAULT_SCHEMA = "public"
DEFAULT_DRIVER = "pg8000"
DEFAULT_IP_TYPE = "private"


@dataclass(frozen=True)
class CloudSqlGatewayConfig:
    """IAM Cloud SQL (Postgres) state backend for checkpoints / run metadata.

    Uses the Cloud SQL Python Connector (cloud-sql-proxy equivalent). No DB password —
    authenticate with the same Google credentials as BigQuery plus ``enable_iam_auth``.
    """

    instance_connection_name: str
    database: str = DEFAULT_DATABASE
    user: Optional[str] = None
    ip_type: str = DEFAULT_IP_TYPE
    schema_name: str = DEFAULT_SCHEMA
    driver: str = DEFAULT_DRIVER
    init_on_connect: bool = True
    # Reserved for future alembic / versioned migrations.
    auto_migrate: bool = True

    def __post_init__(self) -> None:
        if not self.instance_connection_name or self.instance_connection_name.count(":") != 2:
            raise DbtConfigError(
                "gateway.cloudsql.instance_connection_name must look like "
                "'project:region:instance' "
                f"(got {self.instance_connection_name!r})"
            )
        if self.ip_type not in {"private", "public", "psc"}:
            raise DbtConfigError(
                "gateway.cloudsql.ip_type must be one of: private, public, psc "
                f"(got {self.ip_type!r})"
            )


def parse_gateway_config(raw: Optional[Mapping[str, Any]]) -> Optional[CloudSqlGatewayConfig]:
    """Parse ``credentials.gateway`` from profiles.yml. Returns None if unset."""
    if not raw:
        return None
    if not isinstance(raw, Mapping):
        raise DbtConfigError("gateway must be a mapping")

    cloudsql = raw.get("cloudsql")
    if cloudsql is None:
        # Allow either gateway.cloudsql: {...} or a flat cloudsql-shaped mapping later.
        if "instance_connection_name" in raw:
            cloudsql = raw
        else:
            return None

    if not isinstance(cloudsql, Mapping):
        raise DbtConfigError("gateway.cloudsql must be a mapping")

    instance = cloudsql.get("instance_connection_name") or cloudsql.get("instance")
    if not instance:
        raise DbtConfigError(
            "gateway.cloudsql.instance_connection_name is required "
            "(e.g. 'simbe-data-prd:us-central1:metadata')"
        )

    return CloudSqlGatewayConfig(
        instance_connection_name=str(instance),
        database=str(cloudsql.get("database") or DEFAULT_DATABASE),
        user=cloudsql.get("user"),
        ip_type=str(cloudsql.get("ip_type") or DEFAULT_IP_TYPE).lower(),
        schema_name=str(
            cloudsql.get("schema_name") or cloudsql.get("schema") or DEFAULT_SCHEMA
        ),
        driver=str(cloudsql.get("driver") or DEFAULT_DRIVER),
        init_on_connect=bool(cloudsql.get("init_on_connect", True)),
        auto_migrate=bool(cloudsql.get("auto_migrate", True)),
    )


def iam_db_user_from_email(email: str) -> str:
    """Map a Google identity to a Cloud SQL IAM database user name."""
    if not email:
        raise DbtRuntimeError("Cannot derive Cloud SQL IAM user: empty email")
    # service-account@project.iam.gserviceaccount.com -> service-account@project.iam
    if email.endswith(".gserviceaccount.com"):
        return email[: -len(".gserviceaccount.com")]
    return email


def full_target_table_name(database: str, schema: str, identifier: str) -> str:
    """Match Simbe CF / EXTERNAL_QUERY FQN quoting."""
    return f"`{database}`.`{schema}`.`{identifier}`"


def as_plain_dict(config: CloudSqlGatewayConfig) -> Dict[str, Any]:
    return {
        "instance_connection_name": config.instance_connection_name,
        "database": config.database,
        "user": config.user,
        "ip_type": config.ip_type,
        "schema_name": config.schema_name,
        "driver": config.driver,
        "init_on_connect": config.init_on_connect,
        "auto_migrate": config.auto_migrate,
    }
