"""Change-metadata pooler orchestration (BQ CHANGES + gateway Postgres writes)."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence

from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.bigquery.gateway.change_tracking import (
    CHECKPOINT_ADVANCE_STATUSES,
    PartitionMeta,
    build_changes_agg_sql,
    coerce_partition_ids_for_ok,
    dedupe_relation_dicts,
    is_change_history_not_enabled_error,
    is_outside_time_travel_error,
    parse_bq_partition_meta,
    relation_full_name,
)
from dbt.adapters.bigquery.query_parameters import resolve_worker_pool_size

logger = AdapterLogger("BigQuery")


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _is_not_found_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if "not found" in msg and ("table" in msg or "404" in msg):
        return True
    # google.api_core.exceptions.NotFound
    name = type(exc).__name__
    if name == "NotFound":
        return True
    return False


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


class ChangeMetadataPooler:
    """Runs per-relation CHANGES pooling against Cloud SQL gateway state."""

    def __init__(self, adapter: Any):
        self._adapter = adapter

    def run(
        self,
        relations: Sequence[Mapping[str, Any]],
        worker_pool_size: int = 0,
        write_bq: bool = False,
        end_ts: Optional[str] = None,
        invocation_id: Optional[str] = None,
        bq_mirror_table: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        deduped = dedupe_relation_dicts(relations)
        if not deduped:
            return []

        end = end_ts or _utc_now_str()
        inv = invocation_id or getattr(
            getattr(self._adapter, "config", None), "invocation_id", None
        )
        if not inv:
            inv = "00000000-0000-0000-0000-000000000000"

        auto_size = getattr(
            getattr(self._adapter, "config", None), "threads", None
        )
        if auto_size is None:
            auto_size = getattr(
                getattr(self._adapter.connections, "profile", None), "threads", None
            )
        pool_size = resolve_worker_pool_size(
            worker_pool_size, len(deduped), auto_size=auto_size
        )
        logger.debug(
            f"change_metadata_pooler: {len(deduped)} relation(s), "
            f"worker_pool_size={worker_pool_size} "
            f"(resolved={pool_size}, threads={auto_size})"
        )

        results: Dict[int, Dict[str, Any]] = {}
        unexpected: Dict[int, BaseException] = {}

        def _one(index: int, rel: Mapping[str, Any]) -> Dict[str, Any]:
            return self._pool_one(
                rel,
                end_ts=end,
                invocation_id=inv,
                write_bq=write_bq,
                bq_mirror_table=bq_mirror_table,
                worker_index=index,
            )

        with ThreadPoolExecutor(max_workers=pool_size) as executor:
            futures = {
                executor.submit(_one, idx, rel): idx for idx, rel in enumerate(deduped)
            }
            for fut in as_completed(futures):
                idx = futures[fut]
                try:
                    results[idx] = fut.result()
                except BaseException as exc:
                    unexpected[idx] = exc
                    rel = deduped[idx]
                    results[idx] = {
                        "full_table_name": relation_full_name(
                            rel["database"], rel["schema"], rel["identifier"]
                        ),
                        "status": "error",
                        "error": str(exc),
                        "delta_start": None,
                        "delta_end": end,
                        "partition_type": None,
                        "partition_granularity": None,
                        "partition_field": None,
                        "partition_ids": None,
                        "partitions_changed_cnt": None,
                        "rows_changed": None,
                        "rows_insert": None,
                        "rows_update": None,
                        "rows_delete": None,
                    }

        ordered = [results[i] for i in sorted(results)]
        by_status: Dict[str, int] = {}
        for row in ordered:
            st = str(row.get("status") or "unknown")
            by_status[st] = by_status.get(st, 0) + 1
        summary = ", ".join(f"{k}={v}" for k, v in sorted(by_status.items()))
        logger.info(
            f"change_metadata_pooler: finished {len(ordered)} relation(s) ({summary})"
        )
        if unexpected:
            details = "; ".join(
                f"[{i}] {unexpected[i]}" for i in sorted(unexpected)[:10]
            )
            more = len(unexpected) - min(len(unexpected), 10)
            suffix = f" (+{more} more)" if more > 0 else ""
            logger.warning(
                f"change_metadata_pooler: {len(unexpected)}/{len(deduped)} relation(s) "
                f"raised unexpectedly: {details}{suffix}"
            )

        return ordered

    def _pool_one(
        self,
        rel: Mapping[str, Any],
        *,
        end_ts: str,
        invocation_id: str,
        write_bq: bool,
        bq_mirror_table: Optional[str],
        worker_index: int,
    ) -> Dict[str, Any]:
        project = rel["database"]
        dataset = rel["schema"]
        table = rel["identifier"]
        fqn = relation_full_name(project, dataset, table)
        pooled_at = _utc_now_str()
        gateway = self._adapter._get_gateway()
        self._adapter._ensure_gateway_ready()

        conn_mgr = self._adapter.connections
        conn_name = f"change_pooler_{worker_index}"
        conn_mgr.set_connection_name(conn_name)
        try:
            try:
                meta = self._load_partition_meta(project, dataset, table)
            except Exception as exc:
                if _is_not_found_error(exc):
                    # Table not built yet (common in personal schemas) — skip quietly.
                    return {
                        "full_table_name": fqn,
                        "status": "not_found",
                        "delta_start": None,
                        "delta_end": end_ts,
                        "partition_type": None,
                        "partition_granularity": None,
                        "partition_field": None,
                        "partition_ids": None,
                        "partitions_changed_cnt": None,
                        "rows_changed": None,
                        "rows_insert": None,
                        "rows_update": None,
                        "rows_delete": None,
                        "error": str(exc),
                    }
                raise
            ch_enabled = self._ensure_change_history(project, dataset, table)
        finally:
            # Release after metadata/DDL; CHANGES may open the same name again.
            conn_mgr.release()

        cp = gateway.get_pooler_checkpoint(project, dataset, table)
        if cp is None:
            status = "initial"
            delta_start = end_ts
            delta_end = end_ts
            partition_ids: Optional[List[str]] = None
            parts_cnt: Optional[int] = None
            counters = (None, None, None, None)
            err = None
        elif not meta.is_time_partitioned:
            status = "unpartitioned"
            # Window from prior CP end → end_ts
            delta_start = cp.get("delta_end_time") or end_ts
            delta_end = end_ts
            partition_ids = None
            parts_cnt = None
            counters = (None, None, None, None)
            err = None
        else:
            delta_start = cp.get("delta_end_time") or end_ts
            delta_end = end_ts
            conn_mgr.set_connection_name(conn_name)
            try:
                status, partition_ids, parts_cnt, counters, err = self._run_changes(
                    project,
                    dataset,
                    table,
                    meta,
                    delta_start,
                    delta_end,
                    worker_index=worker_index,
                )
            finally:
                conn_mgr.release()

        rows_changed, rows_insert, rows_update, rows_delete = counters
        advance = status in CHECKPOINT_ADVANCE_STATUSES

        gateway.commit_change_tracking_pool_result(
            project=project,
            dataset=dataset,
            table=table,
            pooled_at=pooled_at,
            delta_start_time=delta_start,
            delta_end_time=delta_end,
            partition_type=meta.partition_type,
            partition_granularity=meta.partition_granularity,
            partition_field=meta.partition_field,
            change_history_enabled=ch_enabled,
            rows_changed=rows_changed,
            rows_insert=rows_insert,
            rows_update=rows_update,
            rows_delete=rows_delete,
            partitions_changed_cnt=parts_cnt,
            partition_ids=partition_ids,
            status=status,
            error=err,
            invocation_id=invocation_id,
            run_started_at=pooled_at,
            node_started_at=pooled_at,
            node_finished_at=_utc_now_str(),
            advance_checkpoint=advance,
        )

        result = {
            "full_table_name": fqn,
            "status": status,
            "delta_start": delta_start,
            "delta_end": delta_end,
            "partition_type": meta.partition_type,
            "partition_granularity": meta.partition_granularity,
            "partition_field": meta.partition_field,
            "partition_ids": partition_ids,
            "partitions_changed_cnt": parts_cnt,
            "rows_changed": rows_changed,
            "rows_insert": rows_insert,
            "rows_update": rows_update,
            "rows_delete": rows_delete,
            "error": err,
        }

        if write_bq:
            self._maybe_write_bq_mirror(result, bq_mirror_table)

        return result

    def _load_partition_meta(
        self, project: str, dataset: str, table: str
    ) -> PartitionMeta:
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

    def _ensure_change_history(
        self, project: str, dataset: str, table: str
    ) -> Optional[bool]:
        fqn = relation_full_name(project, dataset, table)
        try:
            enabled = self._is_change_history_enabled(project, dataset, table)
        except Exception as exc:
            logger.debug(f"change_metadata_pooler: CH status check failed for {fqn}: {exc}")
            enabled = None

        if enabled is True:
            return True

        sql = f"ALTER TABLE {fqn} SET OPTIONS(enable_change_history=TRUE)"
        try:
            self._adapter.connections.execute(sql, auto_begin=False, fetch=False)
            logger.info(f"change_metadata_pooler: enabled change history on {fqn}")
            return True
        except Exception as exc:
            logger.warning(
                f"change_metadata_pooler: could not enable change history on {fqn}: {exc}"
            )
            return enabled

    def _is_change_history_enabled(
        self, project: str, dataset: str, table: str
    ) -> Optional[bool]:
        sql = f"""
            SELECT is_change_history_enabled
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table}'
            LIMIT 1
        """
        _, agate_table = self._adapter.connections.execute(sql, auto_begin=False, fetch=True)
        row = _row_dict_from_agate(agate_table)
        if not row:
            return None
        val = next(iter(row.values()))
        if val is None:
            return None
        s = str(val).strip().upper()
        if s in {"YES", "TRUE", "1"}:
            return True
        if s in {"NO", "FALSE", "0"}:
            return False
        return None

    def _run_changes(
        self,
        project: str,
        dataset: str,
        table: str,
        meta: PartitionMeta,
        start_ts: str,
        end_ts: str,
        *,
        worker_index: int,
    ) -> tuple:
        """Return (status, partition_ids, parts_cnt, counters, error)."""
        if start_ts >= end_ts:
            # Empty window — treat as no changes
            return "ok", [], 0, (0, 0, 0, 0), None

        sql = build_changes_agg_sql(project, dataset, table, meta, start_ts, end_ts)
        conn_mgr = self._adapter.connections

        try:
            _, agate_table = conn_mgr.execute(sql, auto_begin=False, fetch=True)
        except Exception as exc:
            if is_change_history_not_enabled_error(exc):
                # Retry once after enable (caller holds thread connection)
                self._ensure_change_history(project, dataset, table)
                try:
                    _, agate_table = conn_mgr.execute(sql, auto_begin=False, fetch=True)
                except Exception as exc2:
                    if is_outside_time_travel_error(exc2):
                        return "out_of_range", None, None, (None, None, None, None), str(exc2)
                    return "error", None, None, (None, None, None, None), str(exc2)
            elif is_outside_time_travel_error(exc):
                return "out_of_range", None, None, (None, None, None, None), str(exc)
            else:
                return "error", None, None, (None, None, None, None), str(exc)

        row = _row_dict_from_agate(agate_table) or {}
        # Empty CHANGES → BQ may return a row of NULLs or no rows
        if not row:
            return "ok", [], 0, (0, 0, 0, 0), None

        raw_ids = row.get("partition_ids")
        partition_ids = coerce_partition_ids_for_ok(raw_ids)
        rows_changed = int(row.get("rows_changed") or 0)
        rows_insert = int(row.get("rows_insert") or 0)
        rows_update = int(row.get("rows_update") or 0)
        rows_delete = int(row.get("rows_delete") or 0)
        # If COUNT(*) is 0, force empty ids
        if rows_changed == 0:
            partition_ids = []
        return (
            "ok",
            partition_ids,
            len(partition_ids),
            (rows_changed, rows_insert, rows_update, rows_delete),
            None,
        )

    def _maybe_write_bq_mirror(
        self, result: Mapping[str, Any], bq_mirror_table: Optional[str]
    ) -> None:
        if not bq_mirror_table:
            logger.debug(
                "change_metadata_pooler: write_bq=True but no bq_mirror_table; skip mirror"
            )
            return
        # Best-effort INSERT; never fail the pool result.
        try:
            ids = result.get("partition_ids")
            if ids is None:
                ids_sql = "CAST(NULL AS ARRAY<STRING>)"
            elif len(ids) == 0:
                ids_sql = "ARRAY<STRING>[]"
            else:
                escaped = ", ".join("'" + str(x).replace("'", "\\'") + "'" for x in ids)
                ids_sql = f"[{escaped}]"

            def _lit(v: Any) -> str:
                if v is None:
                    return "NULL"
                if isinstance(v, (int, float)):
                    return str(v)
                return "'" + str(v).replace("'", "\\'") + "'"

            sql = f"""
            INSERT INTO `{bq_mirror_table}` (
              full_table_name, status, delta_start, delta_end,
              partition_type, partition_granularity, partition_field,
              partition_ids, partitions_changed_cnt,
              rows_changed, rows_insert, rows_update, rows_delete, error, pooled_at
            )
            VALUES (
              {_lit(result.get('full_table_name'))},
              {_lit(result.get('status'))},
              TIMESTAMP({_lit(result.get('delta_start'))}),
              TIMESTAMP({_lit(result.get('delta_end'))}),
              {_lit(result.get('partition_type'))},
              {_lit(result.get('partition_granularity'))},
              {_lit(result.get('partition_field'))},
              {ids_sql},
              {_lit(result.get('partitions_changed_cnt'))},
              {_lit(result.get('rows_changed'))},
              {_lit(result.get('rows_insert'))},
              {_lit(result.get('rows_update'))},
              {_lit(result.get('rows_delete'))},
              {_lit(result.get('error'))},
              CURRENT_TIMESTAMP()
            )
            """
            self._adapter.connections.execute(sql, auto_begin=False, fetch=False)
        except Exception as exc:
            logger.warning(f"change_metadata_pooler: BQ mirror write failed: {exc}")
