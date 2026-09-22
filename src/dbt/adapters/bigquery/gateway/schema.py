"""DDL for the metadata gateway (mirrors simbe data-tf pg_schema).

``migrate()`` is reserved for future versioned upgrades; today we only
``CREATE TABLE / INDEX IF NOT EXISTS`` when objects are missing.
"""

from __future__ import annotations

from typing import Iterable, Sequence

# Same shape as simbe/data-tf/src/pg_schema/dbt_model_log.sql
DBT_MODEL_LOG_TABLE = "dbt_model_log"

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
