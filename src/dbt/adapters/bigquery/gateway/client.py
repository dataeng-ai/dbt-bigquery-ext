from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Dict, Optional, Tuple

from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.credentials import BigQueryCredentials, create_google_credentials
from dbt.adapters.bigquery.gateway.config import (
    CloudSqlGatewayConfig,
    full_target_table_name,
    iam_db_user_from_email,
)
from dbt.adapters.bigquery.gateway import schema as gateway_schema

logger = AdapterLogger("BigQuery")


def _require_connector():
    try:
        from google.cloud.sql.connector import Connector, IPTypes  # noqa: F401
    except ImportError as exc:
        raise DbtRuntimeError(
            "gateway.cloudsql requires cloud-sql-python-connector. "
            'Install with: pip install "cloud-sql-python-connector[pg8000]"'
        ) from exc
    return Connector, IPTypes


def _ip_type(ip_type: str):
    _, IPTypes = _require_connector()
    mapping = {
        "private": IPTypes.PRIVATE,
        "public": IPTypes.PUBLIC,
        "psc": IPTypes.PSC,
    }
    return mapping[ip_type]


def _resolve_iam_user(config: CloudSqlGatewayConfig, credentials: BigQueryCredentials) -> str:
    if config.user:
        return config.user

    if credentials.impersonate_service_account:
        return iam_db_user_from_email(credentials.impersonate_service_account)

    google_creds = create_google_credentials(credentials)
    email = getattr(google_creds, "service_account_email", None)
    if not email:
        # ADC user credentials: prefer signed-in account email when present
        email = getattr(google_creds, "_service_account_email", None)
    if not email and hasattr(google_creds, "signer_email"):
        email = google_creds.signer_email
    if not email:
        # Last resort: refresh and read token info is too heavy; require explicit user.
        raise DbtRuntimeError(
            "gateway.cloudsql.user is required when ADC is not a service account. "
            "Set the Cloud SQL IAM database user (e.g. 'dbt-runner@project.iam')."
        )
    return iam_db_user_from_email(email)


class CloudSqlGateway:
    """Cloud SQL Postgres state backend (checkpoints / dbt_model_log)."""

    def __init__(self, credentials: BigQueryCredentials, config: CloudSqlGatewayConfig):
        self._credentials = credentials
        self._config = config
        self._connector = None
        self._lock = threading.RLock()
        self._initialized = False
        self._iam_user = _resolve_iam_user(config, credentials)

    @property
    def config(self) -> CloudSqlGatewayConfig:
        return self._config

    @property
    def iam_user(self) -> str:
        return self._iam_user

    def _get_connector(self):
        if self._connector is None:
            Connector, _ = _require_connector()
            google_creds = create_google_credentials(self._credentials)
            self._connector = Connector(credentials=google_creds)
        return self._connector

    def connect(self):
        connector = self._get_connector()
        return connector.connect(
            self._config.instance_connection_name,
            self._config.driver,
            user=self._iam_user,
            db=self._config.database,
            enable_iam_auth=True,
            ip_type=_ip_type(self._config.ip_type),
        )

    @contextmanager
    def connection(self):
        conn = self.connect()
        try:
            yield conn
            if hasattr(conn, "commit"):
                conn.commit()
        except Exception:
            if hasattr(conn, "rollback"):
                try:
                    conn.rollback()
                except Exception:
                    pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _table_exists(self, conn, table: str) -> bool:
        cur = conn.cursor()
        try:
            cur.execute(
                gateway_schema.TABLE_EXISTS_SQL,
                (self._config.schema_name, table),
            )
            return cur.fetchone() is not None
        finally:
            cur.close()

    def ensure_schema(self) -> Dict[str, str]:
        """Connect and create missing metadata tables. Skip objects that already exist.

        Returns a map of table name -> 'exists' | 'created'.
        """
        with self._lock:
            status: Dict[str, str] = {}
            with self.connection() as conn:
                cur = conn.cursor()
                try:
                    for table, ddl_templates in gateway_schema.REQUIRED_TABLES:
                        if self._table_exists(conn, table):
                            status[table] = "exists"
                            logger.debug(
                                f"gateway: table {self._config.schema_name}.{table} already exists"
                            )
                            # Still apply non-table DDL (indexes) idempotently.
                            for template in ddl_templates[1:]:
                                sql = gateway_schema.format_ddl(
                                    template, self._config.schema_name, table
                                )
                                cur.execute(sql)
                            continue
                        for template in ddl_templates:
                            sql = gateway_schema.format_ddl(
                                template, self._config.schema_name, table
                            )
                            cur.execute(sql)
                        status[table] = "created"
                        logger.info(
                            f"gateway: created table {self._config.schema_name}.{table}"
                        )
                    if self._config.auto_migrate:
                        gateway_schema.migrate(conn, self._config.schema_name)
                finally:
                    cur.close()
            self._initialized = True
            return status

    def close(self) -> None:
        with self._lock:
            if self._connector is not None:
                try:
                    self._connector.close()
                except Exception:
                    pass
                self._connector = None
            self._initialized = False

    def _qualify(self, table: str = gateway_schema.DBT_MODEL_LOG_TABLE) -> str:
        return f"{self._config.schema_name}.{table}"

    def get_checkpoint(
        self,
        target_database: str,
        target_schema: str,
        target_table_name: str,
    ) -> Optional[Dict[str, Any]]:
        """Latest successful checkpoint for one model (by node_finished_at)."""
        fqn = full_target_table_name(target_database, target_schema, target_table_name)
        sql = f"""
            SELECT
                target_database,
                target_schema,
                target_table_name,
                full_target_table_name,
                invocation_id,
                TO_CHAR(delta_start_time, 'YYYY-MM-DD HH24:MI:SS.US') AS delta_start_time,
                TO_CHAR(delta_end_time, 'YYYY-MM-DD HH24:MI:SS.US') AS delta_end_time,
                TO_CHAR(node_finished_at, 'YYYY-MM-DD HH24:MI:SS.US') AS node_finished_at,
                success
            FROM {self._qualify()}
            WHERE full_target_table_name = %s
              AND success IS TRUE
            ORDER BY node_finished_at DESC NULLS LAST
            LIMIT 1
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (fqn,))
                row = cur.fetchone()
                if not row:
                    return None
                cols = [d[0] for d in cur.description]
                return dict(zip(cols, row))
            finally:
                cur.close()

    def set_checkpoint(
        self,
        invocation_id: str,
        target_database: str,
        target_schema: str,
        target_table_name: str,
        run_started_at: str,
        node_started_at: str,
        node_finished_at: str,
        delta_start_time: str,
        delta_end_time: str,
        success: bool = True,
        full_refresh: Optional[bool] = None,
    ) -> Any:
        """Insert a successful (or failed) checkpoint row. Returns new id."""
        fqn = full_target_table_name(target_database, target_schema, target_table_name)
        sql = f"""
            INSERT INTO {self._qualify()} (
                invocation_id,
                target_database,
                target_schema,
                target_table_name,
                full_target_table_name,
                run_started_at,
                node_started_at,
                node_finished_at,
                success,
                full_refresh,
                delta_start_time,
                delta_end_time
            )
            VALUES (
                %s, %s, %s, %s, %s,
                %s, %s, %s, %s, %s,
                %s, %s
            )
            RETURNING id
        """
        params: Tuple[Any, ...] = (
            invocation_id,
            target_database,
            target_schema,
            target_table_name,
            fqn,
            run_started_at,
            node_started_at,
            node_finished_at,
            success,
            full_refresh,
            delta_start_time,
            delta_end_time,
        )
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                cur.close()
