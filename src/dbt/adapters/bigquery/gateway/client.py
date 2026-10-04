from __future__ import annotations

import threading
from contextlib import contextmanager
from typing import Any, Dict, List, Optional, Sequence, Tuple

from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.bigquery.credentials import BigQueryCredentials, create_google_credentials
from dbt.adapters.bigquery.gateway.config import (
    CloudSqlGatewayConfig,
    full_target_table_name,
    iam_db_user_from_email,
)
from dbt.adapters.bigquery.gateway import schema as gateway_schema
from dbt.adapters.bigquery.gateway.change_tracking import (
    AFFECTED_PARTITION_STATUSES,
    CHECKPOINT_ADVANCE_STATUSES,
    merge_affected_partition_ids,
    pooler_checkpoint_parts,
    relation_full_name,
)

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

    @contextmanager
    def pooler_table_lock(self, project: str, dataset: str, table: str):
        """Session-level advisory lock for one table (survives commits; held until unlock).

        Serializes scheduled / API / ensure-fresh pooling across Cloud Run instances.
        CHANGES runs outside Postgres, so this cannot be a transaction lock.
        """
        fqn = relation_full_name(project, dataset, table)
        sql_lock = (
            "SELECT pg_advisory_lock("
            "('x' || substr(md5(%s), 1, 16))::bit(64)::bigint)"
        )
        sql_unlock = (
            "SELECT pg_advisory_unlock("
            "('x' || substr(md5(%s), 1, 16))::bit(64)::bigint)"
        )
        conn = self.connect()
        try:
            cur = conn.cursor()
            try:
                cur.execute(sql_lock, (fqn,))
                cur.fetchone()
            finally:
                cur.close()
            if hasattr(conn, "commit"):
                conn.commit()
            yield
        finally:
            try:
                cur = conn.cursor()
                try:
                    cur.execute(sql_unlock, (fqn,))
                    cur.fetchone()
                finally:
                    cur.close()
                if hasattr(conn, "commit"):
                    conn.commit()
            except Exception:
                logger.debug("gateway: advisory unlock failed for %s", fqn)
            try:
                conn.close()
            except Exception:
                pass

    def _rollback(self, conn) -> None:
        """Clear an aborted Postgres transaction (e.g. after a failed probe)."""
        if hasattr(conn, "rollback"):
            try:
                conn.rollback()
            except Exception:
                pass

    def _table_exists(self, conn, table: str) -> bool:
        """Return True if the table is listed in information_schema.

        Do **not** probe with ``SELECT … FROM <table>``: a missing table aborts the
        current Postgres transaction (25P02), which then breaks subsequent CREATE.
        """
        cur = conn.cursor()
        try:
            cur.execute(
                gateway_schema.TABLE_EXISTS_SQL,
                (self._config.schema_name, table),
            )
            return cur.fetchone() is not None
        except Exception as exc:
            self._rollback(conn)
            msg = str(exc).lower()
            if "permission denied" in msg or "42501" in str(exc):
                raise DbtRuntimeError(
                    f"gateway: IAM user {self._iam_user!r} cannot access "
                    f"information_schema for {self._config.schema_name}.{table} ({exc}). "
                    f"Grant USAGE on schema {self._config.schema_name} to that role."
                ) from exc
            raise
        finally:
            cur.close()

    def ensure_schema(self) -> Dict[str, str]:
        """Connect and create missing metadata tables; skip objects that already exist.

        When a table already exists (e.g. a shared metadata database), DDL is skipped so
        IAM users without CREATE on the schema are not blocked.

        Returns:
            Map of table name to ``'exists'`` or ``'created'``.
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
                                f"gateway: table {self._config.schema_name}.{table} already exists; skip DDL"
                            )
                            continue
                        try:
                            for template in ddl_templates:
                                sql = gateway_schema.format_ddl(
                                    template, self._config.schema_name, table
                                )
                                cur.execute(sql)
                        except Exception as exc:
                            self._rollback(conn)
                            if self._table_exists(conn, table):
                                status[table] = "exists"
                                logger.warning(
                                    f"gateway: create {self._config.schema_name}.{table} "
                                    f"failed but table exists; continuing ({exc})"
                                )
                                continue
                            raise DbtRuntimeError(
                                f"gateway: cannot create {self._config.schema_name}.{table} "
                                f"({exc}). Grant CREATE on schema {self._config.schema_name} "
                                f"to IAM user {self._iam_user!r}, or create the table as an admin."
                            ) from exc
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

    def get_pooler_checkpoint(
        self, project: str, dataset: str, table: str
    ) -> Optional[Dict[str, Any]]:
        db, schema, identifier = pooler_checkpoint_parts(project, dataset, table)
        return self.get_checkpoint(db, schema, identifier)

    def set_pooler_checkpoint(
        self,
        invocation_id: str,
        project: str,
        dataset: str,
        table: str,
        run_started_at: str,
        node_started_at: str,
        node_finished_at: str,
        delta_start_time: str,
        delta_end_time: str,
        success: bool = True,
    ) -> Any:
        db, schema, identifier = pooler_checkpoint_parts(project, dataset, table)
        return self.set_checkpoint(
            invocation_id=invocation_id,
            target_database=db,
            target_schema=schema,
            target_table_name=identifier,
            run_started_at=run_started_at,
            node_started_at=node_started_at,
            node_finished_at=node_finished_at,
            delta_start_time=delta_start_time,
            delta_end_time=delta_end_time,
            success=success,
            full_refresh=False,
        )

    def upsert_change_tracking_registry(
        self,
        project: str,
        dataset: str,
        table: str,
        partition_type: Optional[str],
        partition_granularity: Optional[str],
        partition_field: Optional[str],
        change_history_enabled: Optional[bool],
        last_status: Optional[str] = None,
        last_pooled_at: Optional[str] = None,
        conn=None,
        *,
        mark_registered: bool = False,
        schedule_group: Optional[str] = None,
    ) -> None:
        fqn = relation_full_name(project, dataset, table)
        reg = self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)
        if mark_registered:
            sql = f"""
                INSERT INTO {reg} (
                    full_table_name, project, dataset, table_name,
                    partition_type, partition_granularity, partition_field,
                    change_history_enabled, enabled, schedule_group,
                    unregistered_at, last_status, last_pooled_at, updated_at
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s, TRUE, %s,
                    NULL, %s, %s, CURRENT_TIMESTAMP
                )
                ON CONFLICT (full_table_name) DO UPDATE SET
                    partition_type = COALESCE(EXCLUDED.partition_type, {reg}.partition_type),
                    partition_granularity = COALESCE(EXCLUDED.partition_granularity, {reg}.partition_granularity),
                    partition_field = COALESCE(EXCLUDED.partition_field, {reg}.partition_field),
                    change_history_enabled = COALESCE(EXCLUDED.change_history_enabled, {reg}.change_history_enabled),
                    enabled = TRUE,
                    schedule_group = COALESCE(EXCLUDED.schedule_group, {reg}.schedule_group),
                    unregistered_at = NULL,
                    last_status = COALESCE(EXCLUDED.last_status, {reg}.last_status),
                    last_pooled_at = COALESCE(EXCLUDED.last_pooled_at, {reg}.last_pooled_at),
                    updated_at = CURRENT_TIMESTAMP
            """
            params = (
                fqn,
                project,
                dataset,
                table,
                partition_type,
                partition_granularity,
                partition_field,
                change_history_enabled,
                schedule_group or "default",
                last_status,
                last_pooled_at,
            )
        else:
            sql = f"""
                INSERT INTO {reg} (
                    full_table_name, project, dataset, table_name,
                    partition_type, partition_granularity, partition_field,
                    change_history_enabled, last_status, last_pooled_at, updated_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, CURRENT_TIMESTAMP)
                ON CONFLICT (full_table_name) DO UPDATE SET
                    partition_type = EXCLUDED.partition_type,
                    partition_granularity = EXCLUDED.partition_granularity,
                    partition_field = EXCLUDED.partition_field,
                    change_history_enabled = EXCLUDED.change_history_enabled,
                    last_status = COALESCE(EXCLUDED.last_status, {reg}.last_status),
                    last_pooled_at = COALESCE(EXCLUDED.last_pooled_at, {reg}.last_pooled_at),
                    updated_at = CURRENT_TIMESTAMP
            """
            params = (
                fqn,
                project,
                dataset,
                table,
                partition_type,
                partition_granularity,
                partition_field,
                change_history_enabled,
                last_status,
                last_pooled_at,
            )
        if conn is not None:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
            finally:
                cur.close()
            return
        with self.connection() as c:
            cur = c.cursor()
            try:
                cur.execute(sql, params)
            finally:
                cur.close()

    def register_change_tracking_table(
        self,
        project: str,
        dataset: str,
        table: str,
        *,
        schedule_group: str = "default",
    ) -> Dict[str, Any]:
        """Upsert registry row as enabled for scheduled pooling."""
        self.upsert_change_tracking_registry(
            project=project,
            dataset=dataset,
            table=table,
            partition_type=None,
            partition_granularity=None,
            partition_field=None,
            change_history_enabled=None,
            mark_registered=True,
            schedule_group=schedule_group,
        )
        return {
            "full_table_name": relation_full_name(project, dataset, table),
            "project": project,
            "dataset": dataset,
            "table": table,
            "enabled": True,
            "schedule_group": schedule_group,
        }

    def unregister_change_tracking_table(
        self, project: str, dataset: str, table: str
    ) -> Dict[str, Any]:
        """Soft-disable a registry row (keep history)."""
        fqn = relation_full_name(project, dataset, table)
        reg = self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)
        sql = f"""
            UPDATE {reg}
            SET enabled = FALSE,
                unregistered_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP
            WHERE full_table_name = %s
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (fqn,))
                updated = cur.rowcount
            finally:
                cur.close()
        return {
            "full_table_name": fqn,
            "project": project,
            "dataset": dataset,
            "table": table,
            "enabled": False,
            "updated": int(updated or 0) > 0,
        }

    def list_change_tracking_registry(
        self,
        *,
        enabled_only: bool = False,
        schedule_group: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        reg = self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)
        clauses = ["1=1"]
        params: List[Any] = []
        if enabled_only:
            clauses.append("enabled = TRUE")
            clauses.append("unregistered_at IS NULL")
        if schedule_group is not None:
            clauses.append("schedule_group = %s")
            params.append(schedule_group)
        where = " AND ".join(clauses)
        sql = f"""
            SELECT full_table_name, project, dataset, table_name,
                   partition_type, partition_granularity, partition_field,
                   change_history_enabled, enabled, schedule_group,
                   registered_at, unregistered_at, last_pooled_at, last_status
            FROM {reg}
            WHERE {where}
            ORDER BY full_table_name
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, tuple(params))
                rows = cur.fetchall()
            finally:
                cur.close()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "full_table_name": r[0],
                    "project": r[1],
                    "dataset": r[2],
                    "table": r[3],
                    "partition_type": r[4],
                    "partition_granularity": r[5],
                    "partition_field": r[6],
                    "change_history_enabled": r[7],
                    "enabled": r[8],
                    "schedule_group": r[9],
                    "registered_at": str(r[10]) if r[10] is not None else None,
                    "unregistered_at": str(r[11]) if r[11] is not None else None,
                    "last_pooled_at": str(r[12]) if r[12] is not None else None,
                    "last_status": r[13],
                }
            )
        return out

    def get_change_tracking_registry_row(
        self, project: str, dataset: str, table: str
    ) -> Optional[Dict[str, Any]]:
        fqn = relation_full_name(project, dataset, table)
        reg = self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)
        sql = f"""
            SELECT full_table_name, project, dataset, table_name,
                   partition_type, partition_granularity, partition_field,
                   change_history_enabled, enabled, schedule_group,
                   registered_at, unregistered_at, last_pooled_at, last_status
            FROM {reg}
            WHERE full_table_name = %s
            LIMIT 1
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (fqn,))
                r = cur.fetchone()
            finally:
                cur.close()
        if not r:
            return None
        return {
            "full_table_name": r[0],
            "project": r[1],
            "dataset": r[2],
            "table": r[3],
            "partition_type": r[4],
            "partition_granularity": r[5],
            "partition_field": r[6],
            "change_history_enabled": r[7],
            "enabled": r[8],
            "schedule_group": r[9],
            "registered_at": str(r[10]) if r[10] is not None else None,
            "unregistered_at": str(r[11]) if r[11] is not None else None,
            "last_pooled_at": str(r[12]) if r[12] is not None else None,
            "last_status": r[13],
        }

    def list_change_tracking_log(
        self,
        project: str,
        dataset: str,
        table: str,
        *,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """Recent pool log rows for one relation (newest first)."""
        fqn = relation_full_name(project, dataset, table)
        lim = max(1, min(int(limit), 500))
        log = self._qualify(gateway_schema.CHANGE_TRACKING_LOG_TABLE)
        sql = f"""
            SELECT id, full_table_name, pooled_at, delta_start_time, delta_end_time,
                   partition_type, partition_granularity, partition_field,
                   rows_changed, rows_insert, rows_update, rows_delete,
                   partitions_changed_cnt, partition_ids,
                   status, error, invocation_id
            FROM {log}
            WHERE full_table_name = %s
            ORDER BY pooled_at DESC, id DESC
            LIMIT {lim}
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (fqn,))
                rows = cur.fetchall()
            finally:
                cur.close()
        out: List[Dict[str, Any]] = []
        for r in rows:
            ids = r[13]
            if ids is not None and not isinstance(ids, list):
                ids = list(ids)
            out.append(
                {
                    "id": r[0],
                    "full_table_name": r[1],
                    "pooled_at": str(r[2]) if r[2] is not None else None,
                    "delta_start_time": str(r[3]) if r[3] is not None else None,
                    "delta_end_time": str(r[4]) if r[4] is not None else None,
                    "partition_type": r[5],
                    "partition_granularity": r[6],
                    "partition_field": r[7],
                    "rows_changed": r[8],
                    "rows_insert": r[9],
                    "rows_update": r[10],
                    "rows_delete": r[11],
                    "partitions_changed_cnt": r[12],
                    "partition_ids": ids,
                    "status": r[14],
                    "error": r[15],
                    "invocation_id": r[16],
                }
            )
        return out

    def start_pooler_run(
        self,
        *,
        started_at: str,
        watermark_to: Optional[str] = None,
        invocation_id: Optional[str] = None,
        trigger: str = "api",
        schedule_group: Optional[str] = None,
        num_tables: int = 0,
    ) -> int:
        sql = f"""
            INSERT INTO {self._qualify(gateway_schema.CHANGE_TRACKING_RUN_TABLE)} (
                started_at, status, watermark_to, invocation_id,
                trigger, schedule_group, num_tables
            )
            VALUES (%s, 'running', %s, %s, %s, %s, %s)
            RETURNING id
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        started_at,
                        watermark_to,
                        invocation_id,
                        trigger,
                        schedule_group,
                        num_tables,
                    ),
                )
                row = cur.fetchone()
                return int(row[0])
            finally:
                cur.close()

    def append_pooler_run_event(
        self,
        run_id: int,
        level: str,
        message: str,
        *,
        full_table_name: Optional[str] = None,
        status: Optional[str] = None,
        logged_at: Optional[str] = None,
    ) -> None:
        sql = f"""
            INSERT INTO {self._qualify(gateway_schema.CHANGE_TRACKING_RUN_EVENT_TABLE)} (
                run_id, logged_at, level, message, full_table_name, status
            )
            VALUES (%s, COALESCE(%s::timestamp, CURRENT_TIMESTAMP), %s, %s, %s, %s)
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        run_id,
                        logged_at,
                        level.upper(),
                        message,
                        full_table_name,
                        status,
                    ),
                )
            finally:
                cur.close()

    def finish_pooler_run(
        self,
        run_id: int,
        *,
        completed_at: str,
        status: str,
        watermark_from: Optional[str] = None,
        watermark_to: Optional[str] = None,
        num_tables: int = 0,
        num_warnings: int = 0,
        num_errors: int = 0,
        summary: Optional[str] = None,
    ) -> None:
        sql = f"""
            UPDATE {self._qualify(gateway_schema.CHANGE_TRACKING_RUN_TABLE)}
            SET completed_at = %s,
                status = %s,
                watermark_from = COALESCE(%s, watermark_from),
                watermark_to = COALESCE(%s, watermark_to),
                num_tables = %s,
                num_warnings = %s,
                num_errors = %s,
                summary = %s
            WHERE id = %s
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (
                        completed_at,
                        status,
                        watermark_from,
                        watermark_to,
                        num_tables,
                        num_warnings,
                        num_errors,
                        summary,
                        run_id,
                    ),
                )
            finally:
                cur.close()

    def list_pooler_runs(self, *, limit: int = 100) -> List[Dict[str, Any]]:
        lim = max(1, min(int(limit), 500))
        sql = f"""
            SELECT id, started_at, completed_at, status,
                   watermark_from, watermark_to,
                   num_tables, num_warnings, num_errors,
                   invocation_id, trigger, schedule_group, summary
            FROM {self._qualify(gateway_schema.CHANGE_TRACKING_RUN_TABLE)}
            ORDER BY started_at DESC, id DESC
            LIMIT {lim}
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql)
                rows = cur.fetchall()
            finally:
                cur.close()
        out: List[Dict[str, Any]] = []
        for r in rows:
            out.append(
                {
                    "id": r[0],
                    "started_at": str(r[1]) if r[1] is not None else None,
                    "completed_at": str(r[2]) if r[2] is not None else None,
                    "status": r[3],
                    "watermark_from": str(r[4]) if r[4] is not None else None,
                    "watermark_to": str(r[5]) if r[5] is not None else None,
                    "num_tables": r[6],
                    "num_warnings": r[7],
                    "num_errors": r[8],
                    "invocation_id": r[9],
                    "trigger": r[10],
                    "schedule_group": r[11],
                    "summary": r[12],
                }
            )
        return out

    def get_pooler_run(self, run_id: int) -> Optional[Dict[str, Any]]:
        sql = f"""
            SELECT id, started_at, completed_at, status,
                   watermark_from, watermark_to,
                   num_tables, num_warnings, num_errors,
                   invocation_id, trigger, schedule_group, summary
            FROM {self._qualify(gateway_schema.CHANGE_TRACKING_RUN_TABLE)}
            WHERE id = %s
            LIMIT 1
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (run_id,))
                r = cur.fetchone()
            finally:
                cur.close()
        if not r:
            return None
        return {
            "id": r[0],
            "started_at": str(r[1]) if r[1] is not None else None,
            "completed_at": str(r[2]) if r[2] is not None else None,
            "status": r[3],
            "watermark_from": str(r[4]) if r[4] is not None else None,
            "watermark_to": str(r[5]) if r[5] is not None else None,
            "num_tables": r[6],
            "num_warnings": r[7],
            "num_errors": r[8],
            "invocation_id": r[9],
            "trigger": r[10],
            "schedule_group": r[11],
            "summary": r[12],
        }

    def list_pooler_run_events(self, run_id: int) -> List[Dict[str, Any]]:
        sql = f"""
            SELECT id, run_id, logged_at, level, message, full_table_name, status
            FROM {self._qualify(gateway_schema.CHANGE_TRACKING_RUN_EVENT_TABLE)}
            WHERE run_id = %s
            ORDER BY id ASC
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (run_id,))
                rows = cur.fetchall()
            finally:
                cur.close()
        return [
            {
                "id": r[0],
                "run_id": r[1],
                "logged_at": str(r[2]) if r[2] is not None else None,
                "level": r[3],
                "message": r[4],
                "full_table_name": r[5],
                "status": r[6],
            }
            for r in rows
        ]

    def upsert_permission_issue(
        self,
        *,
        project: str,
        dataset: str,
        sa_email: str,
        required_role: str,
        scope: str = "dataset",
        last_error: Optional[str] = None,
    ) -> int:
        tbl = self._qualify(gateway_schema.CHANGE_TRACKING_PERMISSION_ISSUE_TABLE)
        sql = f"""
            INSERT INTO {tbl} (
                project, dataset, sa_email, required_role, scope, last_error,
                status, first_seen_at, last_seen_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, 'open', CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
            ON CONFLICT (project, dataset, sa_email, required_role, scope) DO UPDATE SET
                last_error = EXCLUDED.last_error,
                status = 'open',
                last_seen_at = CURRENT_TIMESTAMP,
                resolved_at = NULL
            RETURNING id
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(
                    sql,
                    (project, dataset, sa_email, required_role, scope, last_error),
                )
                row = cur.fetchone()
                return int(row[0])
            finally:
                cur.close()

    def list_permission_issues(
        self, *, status: str = "open", limit: int = 200
    ) -> List[Dict[str, Any]]:
        lim = max(1, min(int(limit), 500))
        tbl = self._qualify(gateway_schema.CHANGE_TRACKING_PERMISSION_ISSUE_TABLE)
        sql = f"""
            SELECT id, project, dataset, sa_email, required_role, scope,
                   last_error, status, first_seen_at, last_seen_at, resolved_at
            FROM {tbl}
            WHERE status = %s
            ORDER BY last_seen_at DESC, id DESC
            LIMIT {lim}
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (status,))
                rows = cur.fetchall()
            finally:
                cur.close()
        return [
            {
                "id": r[0],
                "project": r[1],
                "dataset": r[2],
                "sa_email": r[3],
                "required_role": r[4],
                "scope": r[5],
                "last_error": r[6],
                "status": r[7],
                "first_seen_at": str(r[8]) if r[8] is not None else None,
                "last_seen_at": str(r[9]) if r[9] is not None else None,
                "resolved_at": str(r[10]) if r[10] is not None else None,
            }
            for r in rows
        ]

    def get_permission_issue(self, issue_id: int) -> Optional[Dict[str, Any]]:
        tbl = self._qualify(gateway_schema.CHANGE_TRACKING_PERMISSION_ISSUE_TABLE)
        sql = f"""
            SELECT id, project, dataset, sa_email, required_role, scope,
                   last_error, status, first_seen_at, last_seen_at, resolved_at
            FROM {tbl}
            WHERE id = %s
            LIMIT 1
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (issue_id,))
                r = cur.fetchone()
            finally:
                cur.close()
        if not r:
            return None
        return {
            "id": r[0],
            "project": r[1],
            "dataset": r[2],
            "sa_email": r[3],
            "required_role": r[4],
            "scope": r[5],
            "last_error": r[6],
            "status": r[7],
            "first_seen_at": str(r[8]) if r[8] is not None else None,
            "last_seen_at": str(r[9]) if r[9] is not None else None,
            "resolved_at": str(r[10]) if r[10] is not None else None,
        }

    def resolve_permission_issue(self, issue_id: int) -> None:
        tbl = self._qualify(gateway_schema.CHANGE_TRACKING_PERMISSION_ISSUE_TABLE)
        sql = f"""
            UPDATE {tbl}
            SET status = 'resolved', resolved_at = CURRENT_TIMESTAMP
            WHERE id = %s
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (issue_id,))
            finally:
                cur.close()

    def insert_change_tracking_log(
        self,
        *,
        project: str,
        dataset: str,
        table: str,
        pooled_at: str,
        delta_start_time: str,
        delta_end_time: str,
        partition_type: Optional[str],
        partition_granularity: Optional[str],
        partition_field: Optional[str],
        rows_changed: Optional[int],
        rows_insert: Optional[int],
        rows_update: Optional[int],
        rows_delete: Optional[int],
        partitions_changed_cnt: Optional[int],
        partition_ids: Optional[Sequence[str]],
        status: str,
        error: Optional[str] = None,
        invocation_id: Optional[str] = None,
        conn=None,
    ) -> Any:
        fqn = relation_full_name(project, dataset, table)
        # pg8000 accepts list for TEXT[]; None stays SQL NULL (all).
        ids_param: Any
        if partition_ids is None:
            ids_param = None
        else:
            ids_param = list(partition_ids)

        sql = f"""
            INSERT INTO {self._qualify(gateway_schema.CHANGE_TRACKING_LOG_TABLE)} (
                full_table_name, pooled_at, delta_start_time, delta_end_time,
                partition_type, partition_granularity, partition_field,
                rows_changed, rows_insert, rows_update, rows_delete,
                partitions_changed_cnt, partition_ids,
                status, error, invocation_id
            )
            VALUES (
                %s, %s, %s, %s,
                %s, %s, %s,
                %s, %s, %s, %s,
                %s, %s,
                %s, %s, %s
            )
            RETURNING id
        """
        params = (
            fqn,
            pooled_at,
            delta_start_time,
            delta_end_time,
            partition_type,
            partition_granularity,
            partition_field,
            rows_changed,
            rows_insert,
            rows_update,
            rows_delete,
            partitions_changed_cnt,
            ids_param,
            status,
            error,
            invocation_id,
        )
        if conn is not None:
            cur = conn.cursor()
            try:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                cur.close()

        with self.connection() as c:
            cur = c.cursor()
            try:
                cur.execute(sql, params)
                row = cur.fetchone()
                return row[0] if row else None
            finally:
                cur.close()

    def commit_change_tracking_pool_result(
        self,
        *,
        project: str,
        dataset: str,
        table: str,
        pooled_at: str,
        delta_start_time: str,
        delta_end_time: str,
        partition_type: Optional[str],
        partition_granularity: Optional[str],
        partition_field: Optional[str],
        change_history_enabled: Optional[bool],
        rows_changed: Optional[int],
        rows_insert: Optional[int],
        rows_update: Optional[int],
        rows_delete: Optional[int],
        partitions_changed_cnt: Optional[int],
        partition_ids: Optional[Sequence[str]],
        status: str,
        error: Optional[str],
        invocation_id: str,
        run_started_at: str,
        node_started_at: str,
        node_finished_at: str,
        advance_checkpoint: bool,
    ) -> Any:
        """Insert log + upsert registry (+ optional CP) in one Postgres transaction."""
        with self.connection() as conn:
            log_id = self.insert_change_tracking_log(
                project=project,
                dataset=dataset,
                table=table,
                pooled_at=pooled_at,
                delta_start_time=delta_start_time,
                delta_end_time=delta_end_time,
                partition_type=partition_type,
                partition_granularity=partition_granularity,
                partition_field=partition_field,
                rows_changed=rows_changed,
                rows_insert=rows_insert,
                rows_update=rows_update,
                rows_delete=rows_delete,
                partitions_changed_cnt=partitions_changed_cnt,
                partition_ids=partition_ids,
                status=status,
                error=error,
                invocation_id=invocation_id,
                conn=conn,
            )
            self.upsert_change_tracking_registry(
                project=project,
                dataset=dataset,
                table=table,
                partition_type=partition_type,
                partition_granularity=partition_granularity,
                partition_field=partition_field,
                change_history_enabled=change_history_enabled,
                last_status=status,
                last_pooled_at=pooled_at,
                conn=conn,
            )
            if advance_checkpoint and status in CHECKPOINT_ADVANCE_STATUSES:
                db, schema, identifier = pooler_checkpoint_parts(project, dataset, table)
                fqn = full_target_table_name(db, schema, identifier)
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
                """
                cur = conn.cursor()
                try:
                    cur.execute(
                        sql,
                        (
                            invocation_id,
                            db,
                            schema,
                            identifier,
                            fqn,
                            run_started_at,
                            node_started_at,
                            node_finished_at,
                            True,
                            False,
                            delta_start_time,
                            delta_end_time,
                        ),
                    )
                finally:
                    cur.close()
            return log_id

    def get_affected_partitions(
        self,
        project: str,
        dataset: str,
        table: str,
        start_ts: str,
        end_ts: str,
    ) -> Optional[List[str]]:
        """Return None (all), [] (no changes), or sorted partition id strings."""
        fqn = relation_full_name(project, dataset, table)
        status_list = ", ".join(f"'{s}'" for s in AFFECTED_PARTITION_STATUSES)
        sql = f"""
            SELECT partition_ids
            FROM {self._qualify(gateway_schema.CHANGE_TRACKING_LOG_TABLE)}
            WHERE full_table_name = %s
              AND delta_start_time < %s
              AND delta_end_time > %s
              AND status IN ({status_list})
        """
        with self.connection() as conn:
            cur = conn.cursor()
            try:
                cur.execute(sql, (fqn, end_ts, start_ts))
                rows = cur.fetchall()
            finally:
                cur.close()

        mapped = [{"partition_ids": r[0]} for r in rows]
        return merge_affected_partition_ids(mapped)
