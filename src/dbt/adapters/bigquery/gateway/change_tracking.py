"""Change-metadata pooler helpers (partition meta, CHANGES SQL, merge logic)."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, List, Mapping, Optional, Sequence, Tuple

from dbt.adapters.bigquery.gateway.config import full_target_table_name

POOLER_CHECKPOINT_DATABASE = "gateway.pooler"

# Statuses that count for get_affected_partitions coverage.
AFFECTED_PARTITION_STATUSES = ("ok", "initial", "out_of_range", "unpartitioned")
# Statuses that advance the pooler checkpoint.
CHECKPOINT_ADVANCE_STATUSES = ("initial", "ok", "out_of_range", "unpartitioned")


@dataclass(frozen=True)
class PartitionMeta:
    partition_type: str  # time | ingestion | integer | none
    partition_granularity: Optional[str]  # hour | day | month | year | None
    partition_field: Optional[str]

    @property
    def is_time_partitioned(self) -> bool:
        return self.partition_type in {"time", "ingestion"} and bool(
            self.partition_granularity
        )


def pooler_checkpoint_parts(
    project: str, dataset: str, table: str
) -> Tuple[str, str, str]:
    """Return (target_database, target_schema, target_table_name) for dbt_model_log."""
    return POOLER_CHECKPOINT_DATABASE, project, f"{dataset}.{table}"


def pooler_checkpoint_full_name(project: str, dataset: str, table: str) -> str:
    db, schema, identifier = pooler_checkpoint_parts(project, dataset, table)
    return full_target_table_name(db, schema, identifier)


def relation_full_name(project: str, dataset: str, table: str) -> str:
    return full_target_table_name(project, dataset, table)


def parse_bq_partition_meta(table: Any) -> PartitionMeta:
    """Parse partition info from a google.cloud.bigquery.Table (or duck-typed)."""
    time_part = getattr(table, "time_partitioning", None)
    range_part = getattr(table, "range_partitioning", None)

    if time_part is not None:
        raw_type = getattr(time_part, "type_", None) or getattr(time_part, "type", None)
        grain = _normalize_grain(raw_type)
        field = getattr(time_part, "field", None)
        if field:
            return PartitionMeta(
                partition_type="time",
                partition_granularity=grain,
                partition_field=field,
            )
        # Ingestion-time partitioning
        if grain == "hour":
            part_field = "_PARTITIONTIME"
        else:
            part_field = "_PARTITIONDATE"
        return PartitionMeta(
            partition_type="ingestion",
            partition_granularity=grain or "day",
            partition_field=part_field,
        )

    if range_part is not None:
        field = getattr(range_part, "field", None)
        return PartitionMeta(
            partition_type="integer",
            partition_granularity=None,
            partition_field=field,
        )

    return PartitionMeta(
        partition_type="none",
        partition_granularity=None,
        partition_field=None,
    )


def _normalize_grain(raw: Any) -> Optional[str]:
    if raw is None:
        return "day"
    s = str(raw).strip().lower()
    # BQ / client may return DAY, HOUR, MONTH, YEAR or TimePartitioningType enum
    if "." in s:
        s = s.rsplit(".", 1)[-1]
    if s in {"hour", "day", "month", "year"}:
        return s
    return s or "day"


def partition_id_sql_expr(meta: PartitionMeta) -> Optional[str]:
    """BigQuery SQL expression producing the string partition id."""
    if not meta.is_time_partitioned or not meta.partition_field:
        return None

    field = meta.partition_field
    grain = meta.partition_granularity or "day"

    # Ingestion _PARTITIONDATE is already a DATE; _PARTITIONTIME is TIMESTAMP.
    if field == "_PARTITIONDATE":
        if grain == "month":
            return "FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(_PARTITIONDATE, MONTH))"
        if grain == "year":
            return "FORMAT_DATE('%Y-%m-%d', DATE_TRUNC(_PARTITIONDATE, YEAR))"
        # day (and week if ever used as day-start)
        return "FORMAT_DATE('%Y-%m-%d', _PARTITIONDATE)"

    # TIMESTAMP-like field (_PARTITIONTIME or column)
    if field == "_PARTITIONTIME":
        part_ts = "_PARTITIONTIME"
    else:
        part_ts = f"CAST({field} AS TIMESTAMP)"

    if grain == "hour":
        return (
            f"FORMAT_TIMESTAMP('%Y-%m-%d %H:00:00', TIMESTAMP_TRUNC({part_ts}, HOUR))"
        )
    if grain == "month":
        return (
            f"FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_TRUNC({part_ts}, MONTH)))"
        )
    if grain == "year":
        return f"FORMAT_DATE('%Y-%m-%d', DATE(TIMESTAMP_TRUNC({part_ts}, YEAR)))"
    # day default
    return f"FORMAT_DATE('%Y-%m-%d', DATE({part_ts}))"


def build_changes_agg_sql(
    project: str,
    dataset: str,
    table: str,
    meta: PartitionMeta,
    start_ts: str,
    end_ts: str,
) -> str:
    """Return a single-pass CHANGES aggregation SQL (literal timestamps)."""
    fqn = relation_full_name(project, dataset, table)
    id_expr = partition_id_sql_expr(meta)
    if not id_expr:
        raise ValueError("build_changes_agg_sql requires a time-partitioned table")

    # Escape single quotes in timestamps (ISO-ish strings from callers).
    start = start_ts.replace("'", "\\'")
    end = end_ts.replace("'", "\\'")

    return f"""
SELECT
  ARRAY_AGG(DISTINCT partition_id IGNORE NULLS) AS partition_ids,
  COUNT(*) AS rows_changed,
  COUNTIF(_CHANGE_TYPE = 'INSERT') AS rows_insert,
  COUNTIF(_CHANGE_TYPE = 'UPDATE') AS rows_update,
  COUNTIF(_CHANGE_TYPE = 'DELETE') AS rows_delete
FROM (
  SELECT
    _CHANGE_TYPE,
    {id_expr} AS partition_id
  FROM CHANGES(TABLE {fqn}, '{start}', '{end}')
)
""".strip()


def coerce_partition_ids_for_ok(raw: Any) -> List[str]:
    """BQ ARRAY_AGG on empty → NULL; coerce to [] for status=ok (no changes)."""
    if raw is None:
        return []
    if isinstance(raw, str):
        return [raw]
    return [str(x) for x in raw if x is not None]


def merge_affected_partition_ids(
    rows: Sequence[Mapping[str, Any]],
) -> Optional[List[str]]:
    """Merge overlapping log rows into None (all) | sorted unique ids | [].

    ``rows`` must already be filtered to the query window / statuses.
    Each row needs ``partition_ids`` (None or sequence).
    """
    if not rows:
        return []

    seen: set[str] = set()
    for row in rows:
        ids = row.get("partition_ids")
        if ids is None:
            return None
        for item in ids:
            if item is not None:
                seen.add(str(item))
    return sorted(seen)


def dedupe_relation_dicts(
    relations: Iterable[Mapping[str, Any]],
) -> List[dict]:
    """Dedupe by project.dataset.table; keep first occurrence."""
    out: List[dict] = []
    seen: set[str] = set()
    for rel in relations:
        project = str(rel["database"] if "database" in rel else rel.get("project"))
        dataset = str(rel["schema"] if "schema" in rel else rel.get("dataset"))
        table = str(
            rel.get("identifier") or rel.get("table") or rel.get("table_name")
        )
        key = f"{project}.{dataset}.{table}"
        if key in seen:
            continue
        seen.add(key)
        out.append(
            {
                "database": project,
                "schema": dataset,
                "identifier": table,
                "node_id": rel.get("node_id"),
            }
        )
    return out


def is_outside_time_travel_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if "time travel" in msg:
        return True
    if "outside the" in msg and "window" in msg:
        return True
    if "older than" in msg or "retention period" in msg:
        return True
    if "change history" in msg and "not enabled" not in msg:
        if "window" in msg or "retention" in msg or "older than" in msg:
            return True
    return False


def is_change_history_not_enabled_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    return "change history" in msg and (
        "not enabled" in msg or "enable_change_history" in msg or "disabled" in msg
    )
