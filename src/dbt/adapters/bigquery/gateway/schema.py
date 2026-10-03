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
    registered_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP,
    last_pooled_at TIMESTAMP WITHOUT TIME ZONE,
    last_status VARCHAR(64),
    updated_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT CURRENT_TIMESTAMP
)
"""

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
)


def format_ddl(template: str, schema: str, table: str) -> str:
    return template.format(schema=schema, table=table)


def required_table_names() -> Iterable[str]:
    return (name for name, _ in REQUIRED_TABLES)


def migrate(conn, schema: str) -> None:
    """Reserved: apply versioned migrations.

    Today this is a no-op beyond ensure_schema. Keep the hook so callers and
    profiles can pass ``auto_migrate`` without a breaking change later.
    """
    return None
