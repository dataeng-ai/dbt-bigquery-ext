"""Persist pooler run headers + event log lines into Cloud SQL."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _utc_now_str() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f")


class PoolRunSession:
    def __init__(
        self,
        gateway: Any,
        *,
        trigger: str = "api",
        schedule_group: Optional[str] = None,
        invocation_id: Optional[str] = None,
        watermark_to: Optional[str] = None,
        num_tables: int = 0,
    ):
        self._gw = gateway
        self.started_at = _utc_now_str()
        self.watermark_to = watermark_to or self.started_at
        self.run_id = gateway.start_pooler_run(
            started_at=self.started_at,
            watermark_to=self.watermark_to,
            invocation_id=invocation_id,
            trigger=trigger,
            schedule_group=schedule_group,
            num_tables=num_tables,
        )
        self.num_warnings = 0
        self.num_errors = 0
        self._watermark_from: Optional[str] = None

    def on_event(
        self,
        level: str,
        message: str,
        *,
        full_table_name: Optional[str] = None,
        status: Optional[str] = None,
    ) -> None:
        lvl = (level or "INFO").upper()
        if lvl == "WARNING":
            self.num_warnings += 1
        elif lvl == "ERROR":
            self.num_errors += 1
        self._gw.append_pooler_run_event(
            self.run_id,
            lvl,
            message,
            full_table_name=full_table_name,
            status=status,
        )

    def finish(self, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        completed = _utc_now_str()
        starts = [
            str(r.get("delta_start"))
            for r in results
            if r.get("delta_start") is not None
        ]
        if starts:
            self._watermark_from = min(starts)

        by_status: Dict[str, int] = {}
        for r in results:
            st = str(r.get("status") or "unknown")
            by_status[st] = by_status.get(st, 0) + 1
            if st == "error":
                self.num_errors += 1

        if self.num_errors:
            status = "error"
        elif self.num_warnings:
            status = "warning"
        else:
            status = "ok"

        summary = ", ".join(f"{k}={v}" for k, v in sorted(by_status.items()))
        self._gw.finish_pooler_run(
            self.run_id,
            completed_at=completed,
            status=status,
            watermark_from=self._watermark_from,
            watermark_to=self.watermark_to,
            num_tables=len(results),
            num_warnings=self.num_warnings,
            num_errors=self.num_errors,
            summary=summary or None,
        )
        return {
            "run_id": self.run_id,
            "status": status,
            "num_tables": len(results),
            "num_warnings": self.num_warnings,
            "num_errors": self.num_errors,
            "watermark_from": self._watermark_from,
            "watermark_to": self.watermark_to,
        }
