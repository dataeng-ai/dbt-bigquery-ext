"""DDL for the metadata gateway (checkpoint / change-tracking tables).

``migrate()`` is reserved for future versioned upgrades; today we only
``CREATE TABLE / INDEX IF NOT EXISTS`` when objects are missing.
"""

from __future__ import annotations

from typing import Iterable, Sequence

# Canonical shape for public.dbt_model_log (checkpoint ledger).
DBT_MODEL_LOG_TABLE = "dbt_model_log"
CHANGE_TRACKING_REGISTRY_TABLE = "change_tracking_registry"
CHANGE_TRACKING_LOG_TABLE = "change_tracking_log"
CHANGE_TRACKING_RUN_TABLE = "change_tracking_run"
CHANGE_TRACKING_RUN_EVENT_TABLE = "change_tracking_run_event"
CHANGE_TRACKING_PERMISSION_ISSUE_TABLE = "change_tracking_permission_issue"

CREATE_DBT_MODEL_LOG_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    invocation_id CHAR(36),
    target_database VARCHAR(512),
    target_schema VARCHAR(512),
    target_table_name VARCHAR(512),
    -- should be computed, but CloudSQL doesn't support it
    full_target_table_name VARCHAR(2048),
    run_started_at TIMESTAMP WITHOUT TIME ZONE,
    node_started_at TIMESTAMP WITHOUT TIME ZONE,
    node_finished_at TIMESTAMP WITHOUT TIME ZONE,
    delta_start_time TIMESTAMP WITHOUT TIME ZONE,
    delta_end_time TIMESTAMP WITHOUT TIME ZONE,
    full_refresh BOOLEAN,
    success BOOLEAN,
    _created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_DBT_MODEL_LOG_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS IX_dbt_model_log_full_target_table_name_delta_end_time
ON {schema}.{table} (
    full_target_table_name,
    delta_end_time
)
WHERE success = TRUE
"""

CREATE_CHANGE_TRACKING_REGISTRY_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    full_table_name VARCHAR(2048) PRIMARY KEY,
    project VARCHAR(512) NOT NULL,
    dataset VARCHAR(512) NOT NULL,
    table_name VARCHAR(512) NOT NULL,
    partition_type VARCHAR(64),
    partition_granularity VARCHAR(32),
    partition_field VARCHAR(512),
    change_history_enabled BOOLEAN,
    enabled BOOLEAN NOT NULL DEFAULT TRUE,
    schedule_group VARCHAR(128) NOT NULL DEFAULT 'default',
    registered_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    unregistered_at TIMESTAMP WITHOUT TIME ZONE,
    last_pooled_at TIMESTAMP WITHOUT TIME ZONE,
    last_status VARCHAR(64),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

# Idempotent upgrades for registries created before schedule columns existed.
MIGRATE_REGISTRY_COLUMNS_SQL: Sequence[str] = (
    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS enabled BOOLEAN NOT NULL DEFAULT TRUE",
    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS schedule_group VARCHAR(128) NOT NULL DEFAULT 'default'",
    "ALTER TABLE {schema}.{table} ADD COLUMN IF NOT EXISTS unregistered_at TIMESTAMP WITHOUT TIME ZONE",
)

CREATE_CHANGE_TRACKING_LOG_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    full_table_name VARCHAR(2048) NOT NULL,
    pooled_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    delta_start_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    delta_end_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    partition_type VARCHAR(64),
    partition_granularity VARCHAR(32),
    partition_field VARCHAR(512),
    rows_changed BIGINT,
    rows_insert BIGINT,
    rows_update BIGINT,
    rows_delete BIGINT,
    partitions_changed_cnt INT,
    partition_ids TEXT[],
    status VARCHAR(64) NOT NULL,
    error TEXT,
    invocation_id CHAR(36),
    _created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_CHANGE_TRACKING_LOG_OVERLAP_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS IX_ctl_table_delta_overlap
ON {schema}.{table} (
    full_table_name,
    delta_start_time,
    delta_end_time
)
"""

CREATE_CHANGE_TRACKING_LOG_DELTA_END_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS IX_ctl_table_delta_end
ON {schema}.{table} (
    full_table_name,
    delta_end_time DESC
)
WHERE status IN ('ok', 'initial', 'out_of_range', 'unpartitioned')
"""

CREATE_CHANGE_TRACKING_LOG_PARTITION_IDS_GIN_SQL = """
CREATE INDEX IF NOT EXISTS IX_ctl_partition_ids_gin
ON {schema}.{table} USING GIN (partition_ids)
WHERE partition_ids IS NOT NULL
"""

CREATE_CHANGE_TRACKING_RUN_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    started_at TIMESTAMP WITHOUT TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITHOUT TIME ZONE,
    status VARCHAR(64) NOT NULL DEFAULT 'running',
    watermark_from TIMESTAMP WITHOUT TIME ZONE,
    watermark_to TIMESTAMP WITHOUT TIME ZONE,
    num_tables INT NOT NULL DEFAULT 0,
    num_warnings INT NOT NULL DEFAULT 0,
    num_errors INT NOT NULL DEFAULT 0,
    invocation_id CHAR(36),
    trigger VARCHAR(64),
    schedule_group VARCHAR(128),
    summary TEXT,
    _created_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
)
"""

CREATE_CHANGE_TRACKING_RUN_STARTED_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS IX_ctr_started_at
ON {schema}.{table} (started_at DESC)
"""

CREATE_CHANGE_TRACKING_RUN_EVENT_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    run_id BIGINT NOT NULL REFERENCES {schema}.change_tracking_run(id) ON DELETE CASCADE,
    logged_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    level VARCHAR(16) NOT NULL,
    message TEXT NOT NULL,
    full_table_name VARCHAR(2048),
    status VARCHAR(64)
)
"""

CREATE_CHANGE_TRACKING_RUN_EVENT_RUN_ID_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS IX_ctre_run_id
ON {schema}.{table} (run_id, id)
"""

CREATE_CHANGE_TRACKING_PERMISSION_ISSUE_SQL = """
CREATE TABLE IF NOT EXISTS {schema}.{table} (
    id BIGSERIAL PRIMARY KEY,
    project VARCHAR(512) NOT NULL,
    dataset VARCHAR(512) NOT NULL,
    sa_email VARCHAR(512) NOT NULL,
    required_role VARCHAR(256) NOT NULL,
    scope VARCHAR(32) NOT NULL DEFAULT 'dataset',
    last_error TEXT,
    status VARCHAR(32) NOT NULL DEFAULT 'open',
    first_seen_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_seen_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    resolved_at TIMESTAMP WITHOUT TIME ZONE,
    UNIQUE (project, dataset, sa_email, required_role, scope)
)
"""

CREATE_CHANGE_TRACKING_PERMISSION_ISSUE_STATUS_INDEX_SQL = """
CREATE INDEX IF NOT EXISTS IX_ctpi_status_last_seen
ON {schema}.{table} (status, last_seen_at DESC)
"""

TABLE_EXISTS_SQL = """
SELECT 1
FROM information_schema.tables
WHERE table_schema = %s
  AND table_name = %s
LIMIT 1
"""

# Registry of (name, create statements). Extend when adding tables.
REQUIRED_TABLES: Sequence[tuple[str, Sequence[str]]] = (
    (
        DBT_MODEL_LOG_TABLE,
        (CREATE_DBT_MODEL_LOG_SQL, CREATE_DBT_MODEL_LOG_INDEX_SQL),
    ),
    (
        CHANGE_TRACKING_REGISTRY_TABLE,
        (CREATE_CHANGE_TRACKING_REGISTRY_SQL,),
    ),
    (
        CHANGE_TRACKING_LOG_TABLE,
        (
            CREATE_CHANGE_TRACKING_LOG_SQL,
            CREATE_CHANGE_TRACKING_LOG_OVERLAP_INDEX_SQL,
            CREATE_CHANGE_TRACKING_LOG_DELTA_END_INDEX_SQL,
            CREATE_CHANGE_TRACKING_LOG_PARTITION_IDS_GIN_SQL,
        ),
    ),
    (
        CHANGE_TRACKING_RUN_TABLE,
        (
            CREATE_CHANGE_TRACKING_RUN_SQL,
            CREATE_CHANGE_TRACKING_RUN_STARTED_INDEX_SQL,
        ),
    ),
    (
        CHANGE_TRACKING_RUN_EVENT_TABLE,
        (
            CREATE_CHANGE_TRACKING_RUN_EVENT_SQL,
            CREATE_CHANGE_TRACKING_RUN_EVENT_RUN_ID_INDEX_SQL,
        ),
    ),
    (
        CHANGE_TRACKING_PERMISSION_ISSUE_TABLE,
        (
            CREATE_CHANGE_TRACKING_PERMISSION_ISSUE_SQL,
            CREATE_CHANGE_TRACKING_PERMISSION_ISSUE_STATUS_INDEX_SQL,
        ),
    ),
)


def format_ddl(template: str, schema: str, table: str) -> str:
    return template.format(schema=schema, table=table)


def required_table_names() -> Iterable[str]:
    return (name for name, _ in REQUIRED_TABLES)


def migrate(conn, schema: str) -> None:
    """Apply idempotent schema upgrades (registry schedule columns, etc.).

    Skips statements the current role cannot run (e.g. non-owner ``ALTER TABLE``)
    so shared metadata databases keep working for least-privilege IAM users.
    """
    table = CHANGE_TRACKING_REGISTRY_TABLE
    cur = conn.cursor()
    try:
        for template in MIGRATE_REGISTRY_COLUMNS_SQL:
            sql = format_ddl(template, schema, table)
            try:
                cur.execute(sql)
            except Exception as exc:
                msg = str(exc).lower()
                if (
                    "must be owner" in msg
                    or "42501" in str(exc)
                    or "permission denied" in msg
                ):
                    if hasattr(conn, "rollback"):
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                    continue
                raise
    finally:
        cur.close()
