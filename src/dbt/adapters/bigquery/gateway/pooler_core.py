"""Adapter-free change-metadata pooler (BQ CHANGES + Cloud SQL gateway)."""

from __future__ import annotations

import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
import threading
from typing import (
    Any,
    Callable,
    Dict,
    List,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    runtime_checkable,
)

# level, message, full_table_name?, status?
PoolEventSink = Callable[..., None]

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

logger = logging.getLogger("dbt.adapters.bigquery.gateway.pooler_core")


class PoolerFreshnessError(RuntimeError):
    """ensure_fresh ran (or was needed) but watermark still does not reach ``end_ts``."""

# Fallback when worker_pool_size == 0 and no auto_size provided
_DEFAULT_AUTO_WORKERS = 16


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


def _is_not_found_error(exc: BaseException) -> bool:
    msg = str(exc).lower()
    if "not found" in msg and ("table" in msg or "404" in msg):
        return True
    if type(exc).__name__ == "NotFound":
        return True
    return False


def _ts_compare_lt(a: Optional[str], b: str) -> bool:
    """Return True if a is None or a < b (string compare on ``YYYY-MM-DD HH:MM:SS[.f]``)."""
    if a is None:
        return True
    return str(a) < str(b)


@runtime_checkable
class BqClient(Protocol):
    """Minimal BigQuery surface used by the pooler core."""

    def get_table_meta(self, project: str, dataset: str, table: str) -> PartitionMeta:
        ...

    def run_query_one(self, sql: str) -> Optional[Dict[str, Any]]:
        """Execute SQL and return the first row as a dict, or None if empty."""
        ...

    def execute_sql(self, sql: str) -> None:
        """Execute DDL/DML with no result fetch."""
        ...


class GoogleBqClient:
    """``google.cloud.bigquery.Client`` backed BqClient (Cloud Run / standalone)."""

    def __init__(self, client: Any):
        self._client = client

    def get_table_meta(self, project: str, dataset: str, table: str) -> PartitionMeta:
        bq_table = self._client.get_table(f"{project}.{dataset}.{table}")
        return parse_bq_partition_meta(bq_table)

    def run_query_one(self, sql: str) -> Optional[Dict[str, Any]]:
        job = self._client.query(sql)
        rows = list(job.result())
        if not rows:
            return None
        row = rows[0]
        keys = list(row.keys())
        return {k: row[k] for k in keys}

    def execute_sql(self, sql: str) -> None:
        job = self._client.query(sql)
        list(job.result())


class ChangeMetadataPoolerCore:
    """Runs per-relation CHANGES pooling against Cloud SQL gateway state."""

    def __init__(
        self,
        bq: BqClient,
        gateway: Any,
        *,
        default_threads: Optional[int] = None,
        on_event: Optional[Callable[..., None]] = None,
        pooler_sa_email: Optional[str] = None,
    ):
        self._bq = bq
        self._gateway = gateway
        self._default_threads = default_threads
        self._on_event = on_event
        self._pooler_sa_email = pooler_sa_email
        self._event_lock = threading.Lock()

    def _emit(
        self,
        level: str,
        message: str,
        *,
        full_table_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> None:
        if self._on_event is None:
            return
        with self._event_lock:
            self._on_event(
                level,
                message,
                full_table_name=full_table_name,
                status=status,
            )

    def pool_relations(
        self,
        relations: Sequence[Mapping[str, Any]],
        worker_pool_size: int = 0,
        write_bq: bool = False,
        end_ts: Optional[str] = None,
        invocation_id: Optional[str] = None,
        bq_mirror_table: Optional[str] = None,
        on_event: Optional[Callable[..., None]] = None,
    ) -> List[Dict[str, Any]]:
        if on_event is not None:
            self._on_event = on_event
        deduped = dedupe_relation_dicts(relations)
        if not deduped:
            return []

        end = end_ts or _utc_now_str()
        inv = invocation_id or "00000000-0000-0000-0000-000000000000"
        auto_size = self._default_threads
        pool_size = resolve_worker_pool_size(
            worker_pool_size,
            len(deduped),
            auto_size=auto_size if auto_size else _DEFAULT_AUTO_WORKERS,
        )
        logger.debug(
            "change_metadata_pooler: %s relation(s), worker_pool_size=%s "
            "(resolved=%s, threads=%s)",
            len(deduped),
            worker_pool_size,
            pool_size,
            auto_size,
        )
        self._emit(
            "INFO",
            f"run started: pooling {len(deduped)} relation(s) to end_ts={end}",
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
            self._emit_result_events(row)
        summary = ", ".join(f"{k}={v}" for k, v in sorted(by_status.items()))
        logger.info(
            "change_metadata_pooler: finished %s relation(s) (%s)",
            len(ordered),
            summary,
        )
        self._emit(
            "INFO",
            f"pooler completed: {len(ordered)} relation(s) ({summary})",
        )
        if unexpected:
            details = "; ".join(
                f"[{i}] {unexpected[i]}" for i in sorted(unexpected)[:10]
            )
            more = len(unexpected) - min(len(unexpected), 10)
            suffix = f" (+{more} more)" if more > 0 else ""
            logger.warning(
                "change_metadata_pooler: %s/%s relation(s) raised unexpectedly: %s%s",
                len(unexpected),
                len(deduped),
                details,
                suffix,
            )
        return ordered

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
        """Return partition ids for ``[start_ts, end_ts)``, pooling first if needed.

        When ``ensure_fresh`` and watermark ``W < end_ts``, pools that relation with
        ``end_ts`` then re-reads the log merge.
        """
        cp = self._gateway.get_pooler_checkpoint(project, dataset, table)
        watermark = None
        if cp is not None:
            watermark = cp.get("delta_end_time")
            if watermark is not None:
                watermark = str(watermark)

        pooled = False
        pool_results: List[Dict[str, Any]] = []
        if ensure_fresh and _ts_compare_lt(watermark, end_ts):
            pool_results = self.pool_relations(
                [
                    {
                        "database": project,
                        "schema": dataset,
                        "identifier": table,
                    }
                ],
                worker_pool_size=1,
                end_ts=end_ts,
                invocation_id=invocation_id,
            )
            pooled = True
            cp2 = self._gateway.get_pooler_checkpoint(project, dataset, table)
            watermark = None
            if cp2 is not None and cp2.get("delta_end_time") is not None:
                watermark = str(cp2.get("delta_end_time"))

        coverage = "complete"
        if _ts_compare_lt(watermark, end_ts):
            coverage = "incomplete"

        status_summary = None
        if pool_results:
            status_summary = pool_results[0].get("status")

        fqn = relation_full_name(project, dataset, table)
        if ensure_fresh and coverage == "incomplete":
            err = None
            if pool_results:
                err = pool_results[0].get("error")
            raise PoolerFreshnessError(
                f"change-metadata pooler could not cover [{start_ts}, {end_ts}) for {fqn}: "
                f"watermark={watermark!r} status={status_summary!r} error={err!r}"
            )

        partition_ids = self._gateway.get_affected_partitions(
            project, dataset, table, start_ts, end_ts
        )

        return {
            "partition_ids": partition_ids,
            "watermark": watermark,
            "pooled": pooled,
            "coverage": coverage,
            "status_summary": status_summary,
            "full_table_name": fqn,
            "start_ts": start_ts,
            "end_ts": end_ts,
        }

    def _emit_result_events(self, row: Mapping[str, Any]) -> None:
        fqn = str(row.get("full_table_name") or "")
        status = str(row.get("status") or "")
        err = row.get("error")
        if status == "not_found":
            self._emit(
                "WARNING",
                "table no longer exists, auto disable metadata pooling for it",
                full_table_name=fqn,
                status=status,
            )
            # Soft-disable registry so scheduled pool skips it next time.
            try:
                # fqn is `p`.`d`.`t` — parse via gateway unregister helpers when possible
                parts = [p.strip("`") for p in fqn.replace("`", "").split(".")]
                if len(parts) == 3 and hasattr(
                    self._gateway, "unregister_change_tracking_table"
                ):
                    self._gateway.unregister_change_tracking_table(
                        parts[0], parts[1], parts[2]
                    )
            except Exception as exc:
                self._emit(
                    "WARNING",
                    f"failed to auto-disable missing table: {exc}",
                    full_table_name=fqn,
                    status=status,
                )
        elif status == "out_of_range":
            self._emit(
                "WARNING",
                "time travel is out of range, report NULL partition",
                full_table_name=fqn,
                status=status,
            )
        elif status == "error":
            msg = str(err or "pool error")
            low = msg.lower()
            if "access" in low or "permission" in low or "denied" in low or "403" in low:
                self._emit(
                    "WARNING",
                    f"SA runner doesn't have read access to table: {msg}",
                    full_table_name=fqn,
                    status=status,
                )
                self._record_permission_issue(fqn, msg, for_change_history=False)
            else:
                self._emit(
                    "WARNING",
                    msg,
                    full_table_name=fqn,
                    status=status,
                )

    def _pool_one(
        self,
        rel: Mapping[str, Any],
        *,
        end_ts: str,
        invocation_id: str,
        write_bq: bool,
        bq_mirror_table: Optional[str],
    ) -> Dict[str, Any]:
        project = rel["database"]
        dataset = rel["schema"]
        table = rel["identifier"]
        fqn = relation_full_name(project, dataset, table)
        pooled_at = _utc_now_str()

        lock_cm = getattr(self._gateway, "pooler_table_lock", None)
        if callable(lock_cm):
            with lock_cm(project, dataset, table):
                return self._pool_one_locked(
                    project,
                    dataset,
                    table,
                    fqn=fqn,
                    pooled_at=pooled_at,
                    end_ts=end_ts,
                    invocation_id=invocation_id,
                    write_bq=write_bq,
                    bq_mirror_table=bq_mirror_table,
                )
        return self._pool_one_locked(
            project,
            dataset,
            table,
            fqn=fqn,
            pooled_at=pooled_at,
            end_ts=end_ts,
            invocation_id=invocation_id,
            write_bq=write_bq,
            bq_mirror_table=bq_mirror_table,
        )

    def _pool_one_locked(
        self,
        project: str,
        dataset: str,
        table: str,
        *,
        fqn: str,
        pooled_at: str,
        end_ts: str,
        invocation_id: str,
        write_bq: bool,
        bq_mirror_table: Optional[str],
    ) -> Dict[str, Any]:
        cp = self._gateway.get_pooler_checkpoint(project, dataset, table)
        if cp is not None:
            existing = cp.get("delta_end_time")
            if existing is not None and not _ts_compare_lt(str(existing), end_ts):
                return {
                    "full_table_name": fqn,
                    "status": "skipped",
                    "delta_start": str(existing),
                    "delta_end": str(existing),
                    "partition_type": None,
                    "partition_granularity": None,
                    "partition_field": None,
                    "partition_ids": None,
                    "partitions_changed_cnt": None,
                    "rows_changed": None,
                    "rows_insert": None,
                    "rows_update": None,
                    "rows_delete": None,
                    "error": None,
                }

        try:
            meta = self._bq.get_table_meta(project, dataset, table)
        except Exception as exc:
            if _is_not_found_error(exc):
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
            low = str(exc).lower()
            if "access" in low or "permission" in low or "denied" in low or "403" in low:
                return {
                    "full_table_name": fqn,
                    "status": "error",
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
            delta_start = cp.get("delta_end_time") or end_ts
            delta_end = end_ts
            partition_ids = None
            parts_cnt = None
            counters = (None, None, None, None)
            err = None
        else:
            delta_start = cp.get("delta_end_time") or end_ts
            delta_end = end_ts
            status, partition_ids, parts_cnt, counters, err = self._run_changes(
                project,
                dataset,
                table,
                meta,
                str(delta_start),
                delta_end,
            )

        rows_changed, rows_insert, rows_update, rows_delete = counters
        advance = status in CHECKPOINT_ADVANCE_STATUSES

        self._gateway.commit_change_tracking_pool_result(
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

    def _ensure_change_history(
        self, project: str, dataset: str, table: str
    ) -> Optional[bool]:
        fqn = relation_full_name(project, dataset, table)
        try:
            enabled = self._is_change_history_enabled(project, dataset, table)
        except Exception as exc:
            logger.debug(
                "change_metadata_pooler: CH status check failed for %s: %s", fqn, exc
            )
            enabled = None

        if enabled is True:
            return True

        self._emit(
            "WARNING",
            "table is missing change history, will be enabled",
            full_table_name=fqn,
        )
        sql = f"ALTER TABLE {fqn} SET OPTIONS(enable_change_history=TRUE)"
        try:
            self._bq.execute_sql(sql)
            logger.info("change_metadata_pooler: enabled change history on %s", fqn)
            return True
        except Exception as exc:
            logger.warning(
                "change_metadata_pooler: could not enable change history on %s: %s",
                fqn,
                exc,
            )
            self._emit(
                "WARNING",
                f"could not enable change history: {exc}",
                full_table_name=fqn,
            )
            self._record_permission_issue(fqn, str(exc), for_change_history=True)
            return enabled

    def _record_permission_issue(
        self, fqn: str, error: str, *, for_change_history: bool
    ) -> None:
        if not self._pooler_sa_email or not hasattr(
            self._gateway, "upsert_permission_issue"
        ):
            return
        parts = [p for p in fqn.replace("`", "").split(".") if p]
        if len(parts) < 2:
            return
        project, dataset = parts[0], parts[1]
        role = (
            "roles/bigquery.dataEditor"
            if for_change_history
            else "roles/bigquery.dataViewer"
        )
        try:
            self._gateway.upsert_permission_issue(
                project=project,
                dataset=dataset,
                sa_email=self._pooler_sa_email,
                required_role=role,
                scope="dataset",
                last_error=error[:2000],
            )
        except Exception:
            logger.debug("failed to persist permission issue", exc_info=True)

    def _is_change_history_enabled(
        self, project: str, dataset: str, table: str
    ) -> Optional[bool]:
        sql = f"""
            SELECT is_change_history_enabled
            FROM `{project}.{dataset}.INFORMATION_SCHEMA.TABLES`
            WHERE table_name = '{table}'
            LIMIT 1
        """
        row = self._bq.run_query_one(sql)
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
    ) -> tuple:
        if start_ts >= end_ts:
            return "ok", [], 0, (0, 0, 0, 0), None

        sql = build_changes_agg_sql(project, dataset, table, meta, start_ts, end_ts)

        try:
            row = self._bq.run_query_one(sql)
        except Exception as exc:
            if is_change_history_not_enabled_error(exc):
                self._ensure_change_history(project, dataset, table)
                try:
                    row = self._bq.run_query_one(sql)
                except Exception as exc2:
                    if is_outside_time_travel_error(exc2):
                        return (
                            "out_of_range",
                            None,
                            None,
                            (None, None, None, None),
                            str(exc2),
                        )
                    return "error", None, None, (None, None, None, None), str(exc2)
            elif is_outside_time_travel_error(exc):
                return "out_of_range", None, None, (None, None, None, None), str(exc)
            else:
                return "error", None, None, (None, None, None, None), str(exc)

        if not row:
            return "ok", [], 0, (0, 0, 0, 0), None

        raw_ids = row.get("partition_ids")
        partition_ids = coerce_partition_ids_for_ok(raw_ids)
        rows_changed = int(row.get("rows_changed") or 0)
        rows_insert = int(row.get("rows_insert") or 0)
        rows_update = int(row.get("rows_update") or 0)
        rows_delete = int(row.get("rows_delete") or 0)
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
                "change_metadata_pooler: write_bq=True but no bq_mirror_table; skip"
            )
            return
        try:
            ids = result.get("partition_ids")
            if ids is None:
                ids_sql = "CAST(NULL AS ARRAY<STRING>)"
            elif len(ids) == 0:
                ids_sql = "ARRAY<STRING>[]"
            else:
                escaped = ", ".join(
                    "'" + str(x).replace("'", "\\'") + "'" for x in ids
                )
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
            self._bq.execute_sql(sql)
        except Exception as exc:
            logger.warning("change_metadata_pooler: BQ mirror write failed: %s", exc)
