import unittest
from unittest.mock import MagicMock, patch

from dbt_common.exceptions import DbtConfigError, DbtRuntimeError

from dbt.adapters.bigquery.gateway.config import (
    CloudSqlGatewayConfig,
    full_target_table_name,
    iam_db_user_from_email,
    parse_gateway_config,
)
from dbt.adapters.bigquery.gateway.client import CloudSqlGateway
from dbt.adapters.bigquery.gateway import schema as gateway_schema


class TestGatewayConfig(unittest.TestCase):
    def test_parse_none(self):
        self.assertIsNone(parse_gateway_config(None))
        self.assertIsNone(parse_gateway_config({}))

    def test_parse_cloudsql(self):
        cfg = parse_gateway_config(
            {
                "cloudsql": {
                    "instance_connection_name": "my-gcp-project:us-central1:metadata",
                    "database": "metadata",
                    "ip_type": "private",
                }
            }
        )
        self.assertIsInstance(cfg, CloudSqlGatewayConfig)
        self.assertEqual(
            cfg.instance_connection_name, "my-gcp-project:us-central1:metadata"
        )
        self.assertEqual(cfg.database, "metadata")
        self.assertTrue(cfg.init_on_connect)
        self.assertTrue(cfg.auto_migrate)

    def test_invalid_instance(self):
        with self.assertRaises(DbtConfigError):
            parse_gateway_config({"cloudsql": {"instance_connection_name": "bad"}})

    def test_iam_user_from_sa_email(self):
        self.assertEqual(
            iam_db_user_from_email(
                "dbt-runner@my-gcp-project.iam.gserviceaccount.com"
            ),
            "dbt-runner@my-gcp-project.iam",
        )

    def test_full_target_table_name(self):
        self.assertEqual(
            full_target_table_name("my-gcp-project", "analytics", "fact_jobs"),
            "`my-gcp-project`.`analytics`.`fact_jobs`",
        )


class TestGatewaySchemaEnsure(unittest.TestCase):
    def test_ensure_creates_when_missing(self):
        creds = MagicMock()
        creds.impersonate_service_account = (
            "dbt-runner@my-gcp-project.iam.gserviceaccount.com"
        )
        cfg = CloudSqlGatewayConfig(
            instance_connection_name="my-gcp-project:us-central1:metadata"
        )

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        # information_schema: no row → missing → CREATE
        cur.fetchone.return_value = None

        gw = CloudSqlGateway(creds, cfg)
        with patch.object(gw, "connect", return_value=conn):
            status = gw.ensure_schema()

        self.assertEqual(status[gateway_schema.DBT_MODEL_LOG_TABLE], "created")
        self.assertEqual(
            status[gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE], "created"
        )
        self.assertEqual(status[gateway_schema.CHANGE_TRACKING_LOG_TABLE], "created")
        executed_sql = [c.args[0] for c in cur.execute.call_args_list]
        self.assertTrue(any("information_schema.tables" in sql.lower() for sql in executed_sql))
        self.assertTrue(any("create table" in sql.lower() for sql in executed_sql))
        # Never probe missing tables with SELECT FROM <table> (aborts txn → 25P02)
        self.assertFalse(
            any(
                "select 1 from public.dbt_model_log" in sql.lower()
                or "select 1 from public.change_tracking" in sql.lower()
                for sql in executed_sql
            )
        )

    def test_ensure_skips_when_exists(self):
        creds = MagicMock()
        creds.impersonate_service_account = (
            "dbt-runner@my-gcp-project.iam.gserviceaccount.com"
        )
        cfg = CloudSqlGatewayConfig(
            instance_connection_name="my-gcp-project:us-central1:metadata"
        )

        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = (1,)

        gw = CloudSqlGateway(creds, cfg)
        with patch.object(gw, "connect", return_value=conn):
            status = gw.ensure_schema()

        self.assertEqual(status[gateway_schema.DBT_MODEL_LOG_TABLE], "exists")
        self.assertEqual(
            status[gateway_schema.CHANGE_TRACKING_REGISTRY_TABLE], "exists"
        )
        self.assertEqual(status[gateway_schema.CHANGE_TRACKING_LOG_TABLE], "exists")
        executed_sql = [c.args[0] for c in cur.execute.call_args_list]
        self.assertTrue(
            any("information_schema.tables" in sql.lower() for sql in executed_sql)
        )
        self.assertFalse(any("create table" in sql.lower() for sql in executed_sql))
        self.assertFalse(any("create index" in sql.lower() for sql in executed_sql))

    def test_get_affected_partitions_null_vs_empty(self):
        creds = MagicMock()
        creds.impersonate_service_account = (
            "dbt-runner@my-gcp-project.iam.gserviceaccount.com"
        )
        cfg = CloudSqlGatewayConfig(
            instance_connection_name="my-gcp-project:us-central1:metadata"
        )
        gw = CloudSqlGateway(creds, cfg)
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur

        # One NULL marker → all
        cur.fetchall.return_value = [(None,), (["2026-09-20"],)]
        with patch.object(gw, "connect", return_value=conn):
            self.assertIsNone(
                gw.get_affected_partitions(
                    "my-gcp-project",
                    "analytics",
                    "orders",
                    "2026-09-20 00:00:00",
                    "2026-09-21 00:00:00",
                )
            )

        # Only empty arrays → no changes
        cur.fetchall.return_value = [([],), ([],)]
        with patch.object(gw, "connect", return_value=conn):
            self.assertEqual(
                gw.get_affected_partitions(
                    "my-gcp-project",
                    "analytics",
                    "orders",
                    "2026-09-20 00:00:00",
                    "2026-09-21 00:00:00",
                ),
                [],
            )


class TestGatewayCheckpoints(unittest.TestCase):
    def _gateway(self):
        creds = MagicMock()
        creds.impersonate_service_account = (
            "dbt-runner@my-gcp-project.iam.gserviceaccount.com"
        )
        cfg = CloudSqlGatewayConfig(
            instance_connection_name="my-gcp-project:us-central1:metadata"
        )
        return CloudSqlGateway(creds, cfg)

    def test_get_checkpoint_none(self):
        gw = self._gateway()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = None
        cur.description = []

        with patch.object(gw, "connect", return_value=conn):
            self.assertIsNone(
                gw.get_checkpoint("my-gcp-project", "analytics", "fact_jobs")
            )

    def test_set_checkpoint_insert(self):
        gw = self._gateway()
        conn = MagicMock()
        cur = MagicMock()
        conn.cursor.return_value = cur
        cur.fetchone.return_value = (42,)

        with patch.object(gw, "connect", return_value=conn):
            row_id = gw.set_checkpoint(
                invocation_id="aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee",
                target_database="my-gcp-project",
                target_schema="analytics",
                target_table_name="fact_jobs",
                run_started_at="2026-09-21 12:00:00.000000",
                node_started_at="2026-09-21 12:00:01.000000",
                node_finished_at="2026-09-21 12:00:02.000000",
                delta_start_time="2026-07-14 04:35:00.000000",
                delta_end_time="2026-09-21 12:00:00.000000",
            )
        self.assertEqual(row_id, 42)
        sql = cur.execute.call_args.args[0]
        self.assertIn("INSERT INTO public.dbt_model_log", sql)

    def test_missing_connector_package(self):
        gw = self._gateway()
        with patch(
            "dbt.adapters.bigquery.gateway.client._require_connector",
            side_effect=DbtRuntimeError("missing"),
        ):
            with self.assertRaises(DbtRuntimeError):
                gw.connect()


if __name__ == "__main__":
    unittest.main()
