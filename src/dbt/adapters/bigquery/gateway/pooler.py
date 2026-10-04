"""Change-metadata pooler — thin dbt-adapter wrapper around pooler_core."""

from __future__ import annotations

from typing import Any, Dict, List, Mapping, Optional, Sequence

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.bigquery.gateway.change_tracking import (
    PartitionMeta,
    parse_bq_partition_meta,
)
from dbt.adapters.bigquery.gateway.pooler_core import ChangeMetadataPoolerCore

logger = AdapterLogger("BigQuery")


def _row_dict_from_agate(table: Any) -> Optional[Dict[str, Any]]:
    if table is None:
        return None
    try:
        if not getattr(table, "rows", None) and len(table) == 0:
            return None
        if hasattr(table, "rows") and len(table.rows) == 0:
            return None
        row = table.rows[0] if hasattr(table, "rows") else table[0]
        names = list(table.column_names)
        return {name: row[name] for name in names}
    except Exception:
        return None


class AdapterBqClient:
    """BqClient that uses the dbt BigQuery adapter connection manager."""

    def __init__(self, adapter: Any):
        self._adapter = adapter

    def get_table_meta(self, project: str, dataset: str, table: str) -> PartitionMeta:
        Relation = self._adapter.Relation
        relation = Relation.create(
            database=project,
            schema=dataset,
            identifier=table,
            type="table",
            quote_policy={"database": True, "schema": True, "identifier": True},
        )
        conn = self._adapter.connections.get_thread_connection()
        client = conn.handle
        table_ref = self._adapter.get_table_ref_from_relation(relation)
        bq_table = client.get_table(table_ref)
        return parse_bq_partition_meta(bq_table)

    def run_query_one(self, sql: str) -> Optional[Dict[str, Any]]:
        _, agate_table = self._adapter.connections.execute(
            sql, auto_begin=False, fetch=True
        )
        return _row_dict_from_agate(agate_table)

    def execute_sql(self, sql: str) -> None:
        self._adapter.connections.execute(sql, auto_begin=False, fetch=False)


class ChangeMetadataPooler:
    """Runs per-relation CHANGES pooling against Cloud SQL gateway state."""

    def __init__(self, adapter: Any):
        self._adapter = adapter
        auto_size = getattr(getattr(adapter, "config", None), "threads", None)
        if auto_size is None:
            auto_size = getattr(
                getattr(adapter.connections, "profile", None), "threads", None
            )
        self._core = ChangeMetadataPoolerCore(
            AdapterBqClient(adapter),
            adapter._get_gateway(),
            default_threads=auto_size,
        )

    def run(
        self,
        relations: Sequence[Mapping[str, Any]],
        worker_pool_size: int = 0,
        write_bq: bool = False,
        end_ts: Optional[str] = None,
        invocation_id: Optional[str] = None,
        bq_mirror_table: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        self._adapter._ensure_gateway_ready()
        return self._core.pool_relations(
            relations=relations,
            worker_pool_size=worker_pool_size,
            write_bq=write_bq,
            end_ts=end_ts,
            invocation_id=invocation_id,
            bq_mirror_table=bq_mirror_table,
        )

    def ensure_affected_partitions(
        self,
        project: str,
        dataset: str,
        table: str,
        start_ts: str,
        end_ts: str,
        *,
        ensure_fresh: bool = True,
        invocation_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        self._adapter._ensure_gateway_ready()
        return self._core.ensure_affected_partitions(
            project,
            dataset,
            table,
            start_ts,
            end_ts,
            ensure_fresh=ensure_fresh,
            invocation_id=invocation_id,
        )
