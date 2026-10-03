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

    def _table_exists(self, conn, table: str) -> bool:
        """Return True if the table is visible via SELECT (not only information_schema)."""
        cur = conn.cursor()
        try:
            cur.execute(
                f"SELECT 1 FROM {self._config.schema_name}.{table} LIMIT 0"
            )
            return True
        except Exception as exc:
            msg = str(exc).lower()
            if "does not exist" in msg or "undefinedtable" in msg.replace(" ", ""):
                return False
            if "permission denied" in msg or "42501" in str(exc):
                raise DbtRuntimeError(
                    f"gateway: IAM user {self._iam_user!r} cannot access "
                    f"{self._config.schema_name}.{table} ({exc}). "
                    f'Grant SELECT, INSERT (and USAGE on schema {self._config.schema_name}) '
                    f"to that role."
                ) from exc
            try:
                cur.execute(
                    gateway_schema.TABLE_EXISTS_SQL,
                    (self._config.schema_name, table),
                )
                return cur.fetchone() is not None
            except Exception:
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
    ) -> None:
        fqn = relation_full_name(project, dataset, table)
        sql = f"""
            INSERT INTO {self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)} (
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
                last_status = COALESCE(EXCLUDED.last_status, {self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)}.last_status),
                last_pooled_at = COALESCE(EXCLUDED.last_pooled_at, {self._qualify(gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE)}.last_pooled_at),
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
