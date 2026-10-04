"""Unit tests for adapter-free change-metadata pooler core."""

from __future__ import annotations

import unittest
from typing import Any, Dict, List, Optional
from unittest.mock import MagicMock

from dbt.adapters.bigquery.gateway.change_tracking import PartitionMeta
from dbt.adapters.bigquery.gateway.pooler_core import ChangeMetadataPoolerCore


class FakeBq:
    def __init__(self, meta: Optional[PartitionMeta] = None):
        self.meta = meta or PartitionMeta("time", "day", "dt")
        self.queries: List[str] = []
        self.execs: List[str] = []
        self.query_one_result: Optional[Dict[str, Any]] = {
            "partition_ids": ["2026-09-20"],
            "rows_changed": 1,
            "rows_insert": 1,
            "rows_update": 0,
            "rows_delete": 0,
        }

    def get_table_meta(self, project, dataset, table):
        return self.meta

    def run_query_one(self, sql: str):
        self.queries.append(sql)
        if "INFORMATION_SCHEMA" in sql:
            return {"is_change_history_enabled": "YES"}
        return self.query_one_result

    def execute_sql(self, sql: str) -> None:
        self.execs.append(sql)


class TestEnsureAffectedPartitions(unittest.TestCase):
    def test_pools_when_watermark_behind(self):
        gw = MagicMock()
        # First ensure check is behind; subsequent reads (inside pool + re-read) are fresh.
        calls = {"n": 0}

        def _cp(*_a, **_k):
            calls["n"] += 1
            if calls["n"] == 1:
                return {"delta_end_time": "2026-09-20 00:00:00.000000"}
            return {"delta_end_time": "2026-09-21 00:00:00.000000"}

        gw.get_pooler_checkpoint.side_effect = _cp
        gw.get_affected_partitions.return_value = ["2026-09-20"]
        gw.commit_change_tracking_pool_result.return_value = 1

        core = ChangeMetadataPoolerCore(FakeBq(), gw, default_threads=1)
        out = core.ensure_affected_partitions(
            "my-gcp-project",
            "analytics",
            "orders",
            "2026-09-19 00:00:00",
            "2026-09-21 00:00:00",
            ensure_fresh=True,
        )
        self.assertTrue(out["pooled"])
        self.assertEqual(out["coverage"], "complete")
        self.assertEqual(out["partition_ids"], ["2026-09-20"])
        self.assertEqual(gw.commit_change_tracking_pool_result.call_count, 1)

    def test_skips_pool_when_fresh(self):
        gw = MagicMock()
        gw.get_pooler_checkpoint.return_value = {
            "delta_end_time": "2026-09-21 00:00:00.000000"
        }
        gw.get_affected_partitions.return_value = []
        core = ChangeMetadataPoolerCore(FakeBq(), gw, default_threads=1)
        out = core.ensure_affected_partitions(
            "my-gcp-project",
            "analytics",
            "orders",
            "2026-09-19 00:00:00",
            "2026-09-21 00:00:00",
            ensure_fresh=True,
        )
        self.assertFalse(out["pooled"])
        self.assertEqual(out["coverage"], "complete")
        gw.commit_change_tracking_pool_result.assert_not_called()

    def test_incomplete_when_ensure_fresh_false(self):
        gw = MagicMock()
        gw.get_pooler_checkpoint.return_value = {
            "delta_end_time": "2026-09-20 00:00:00.000000"
        }
        gw.get_affected_partitions.return_value = []
        core = ChangeMetadataPoolerCore(FakeBq(), gw, default_threads=1)
        out = core.ensure_affected_partitions(
            "my-gcp-project",
            "analytics",
            "orders",
            "2026-09-19 00:00:00",
            "2026-09-21 00:00:00",
            ensure_fresh=False,
        )
        self.assertFalse(out["pooled"])
        self.assertEqual(out["coverage"], "incomplete")


class TestPoolInitial(unittest.TestCase):
    def test_initial_checkpoint(self):
        gw = MagicMock()
        gw.get_pooler_checkpoint.return_value = None
        core = ChangeMetadataPoolerCore(FakeBq(), gw, default_threads=1)
        results = core.pool_relations(
            [
                {
                    "database": "my-gcp-project",
                    "schema": "analytics",
                    "identifier": "orders",
                }
            ],
            worker_pool_size=1,
            end_ts="2026-09-21 00:00:00.000000",
        )
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]["status"], "initial")
        gw.commit_change_tracking_pool_result.assert_called_once()


if __name__ == "__main__":
    unittest.main()
