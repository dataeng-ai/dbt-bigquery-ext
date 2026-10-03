import unittest
from types import SimpleNamespace

from dbt.adapters.bigquery.gateway.change_tracking import (
    PartitionMeta,
    build_changes_agg_sql,
    coerce_partition_ids_for_ok,
    dedupe_relation_dicts,
    merge_affected_partition_ids,
    parse_bq_partition_meta,
    partition_id_sql_expr,
    pooler_checkpoint_full_name,
    pooler_checkpoint_parts,
)


class TestPoolerCheckpointKeys(unittest.TestCase):
    def test_parts_and_full_name(self):
        db, schema, table = pooler_checkpoint_parts(
            "my-gcp-project", "analytics", "orders"
        )
        self.assertEqual(db, "gateway.pooler")
        self.assertEqual(schema, "my-gcp-project")
        self.assertEqual(table, "analytics.orders")
        self.assertEqual(
            pooler_checkpoint_full_name("my-gcp-project", "analytics", "orders"),
            "`gateway.pooler`.`my-gcp-project`.`analytics.orders`",
        )


class TestPartitionMeta(unittest.TestCase):
    def test_time_day_field(self):
        table = SimpleNamespace(
            time_partitioning=SimpleNamespace(type_="DAY", field="created_at"),
            range_partitioning=None,
        )
        meta = parse_bq_partition_meta(table)
        self.assertEqual(meta.partition_type, "time")
        self.assertEqual(meta.partition_granularity, "day")
        self.assertEqual(meta.partition_field, "created_at")
        self.assertTrue(meta.is_time_partitioned)

    def test_ingestion_hour(self):
        table = SimpleNamespace(
            time_partitioning=SimpleNamespace(type_="HOUR", field=None),
            range_partitioning=None,
        )
        meta = parse_bq_partition_meta(table)
        self.assertEqual(meta.partition_type, "ingestion")
        self.assertEqual(meta.partition_granularity, "hour")
        self.assertEqual(meta.partition_field, "_PARTITIONTIME")

    def test_integer_range(self):
        table = SimpleNamespace(
            time_partitioning=None,
            range_partitioning=SimpleNamespace(field="customer_id"),
        )
        meta = parse_bq_partition_meta(table)
        self.assertEqual(meta.partition_type, "integer")
        self.assertFalse(meta.is_time_partitioned)

    def test_none(self):
        table = SimpleNamespace(time_partitioning=None, range_partitioning=None)
        meta = parse_bq_partition_meta(table)
        self.assertEqual(meta.partition_type, "none")


class TestPartitionIdExpr(unittest.TestCase):
    def test_hour(self):
        meta = PartitionMeta("time", "hour", "event_ts")
        expr = partition_id_sql_expr(meta)
        self.assertIn("TIMESTAMP_TRUNC", expr)
        self.assertIn("%Y-%m-%d %H:00:00", expr)

    def test_day(self):
        meta = PartitionMeta("time", "day", "event_ts")
        expr = partition_id_sql_expr(meta)
        self.assertIn("FORMAT_DATE('%Y-%m-%d'", expr)

    def test_month(self):
        meta = PartitionMeta("ingestion", "month", "_PARTITIONDATE")
        expr = partition_id_sql_expr(meta)
        self.assertIn("DATE_TRUNC(_PARTITIONDATE, MONTH)", expr)


class TestChangesSql(unittest.TestCase):
    def test_contains_changes_and_counters(self):
        meta = PartitionMeta("time", "day", "dt")
        sql = build_changes_agg_sql(
            "my-gcp-project",
            "analytics",
            "orders",
            meta,
            "2026-09-20 10:00:00.000000",
            "2026-09-21 10:00:00.000000",
        )
        self.assertIn("CHANGES(TABLE `my-gcp-project`.`analytics`.`orders`", sql)
        self.assertIn("COUNTIF(_CHANGE_TYPE = 'INSERT')", sql)
        self.assertIn("COUNTIF(_CHANGE_TYPE = 'UPDATE')", sql)
        self.assertIn("COUNTIF(_CHANGE_TYPE = 'DELETE')", sql)
        self.assertIn("ARRAY_AGG(DISTINCT partition_id IGNORE NULLS)", sql)


class TestPartitionIdsSemantics(unittest.TestCase):
    def test_coerce_null_to_empty_for_ok(self):
        self.assertEqual(coerce_partition_ids_for_ok(None), [])
        self.assertEqual(coerce_partition_ids_for_ok(["2026-09-20"]), ["2026-09-20"])

    def test_merge_null_means_all(self):
        self.assertIsNone(
            merge_affected_partition_ids(
                [{"partition_ids": ["2026-09-20"]}, {"partition_ids": None}]
            )
        )

    def test_merge_all_empty_means_no_changes(self):
        self.assertEqual(
            merge_affected_partition_ids(
                [{"partition_ids": []}, {"partition_ids": []}]
            ),
            [],
        )

    def test_merge_union_sorted(self):
        self.assertEqual(
            merge_affected_partition_ids(
                [
                    {"partition_ids": ["2026-09-21", "2026-09-19"]},
                    {"partition_ids": ["2026-09-20"]},
                ]
            ),
            ["2026-09-19", "2026-09-20", "2026-09-21"],
        )

    def test_merge_no_rows(self):
        self.assertEqual(merge_affected_partition_ids([]), [])


class TestDedupeRelations(unittest.TestCase):
    def test_dedupe_keeps_first(self):
        out = dedupe_relation_dicts(
            [
                {
                    "database": "p",
                    "schema": "d",
                    "identifier": "t",
                    "node_id": "a",
                },
                {
                    "database": "p",
                    "schema": "d",
                    "identifier": "t",
                    "node_id": "b",
                },
                {
                    "database": "p",
                    "schema": "d",
                    "identifier": "u",
                    "node_id": "c",
                },
            ]
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["node_id"], "a")
        self.assertEqual(out[1]["identifier"], "u")


if __name__ == "__main__":
    unittest.main()
