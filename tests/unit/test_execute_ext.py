import datetime
import unittest
from unittest.mock import MagicMock, Mock, patch

from google.cloud.bigquery import ScalarQueryParameter

from dbt_common.exceptions import DbtRuntimeError
from dbt.adapters.bigquery.connections import BigQueryConnectionManager
from dbt.adapters.bigquery.query_parameters import (
    AUTO_WORKER_POOL_SIZE,
    MAX_WORKER_POOL_SIZE,
    build_query_parameters,
    infer_scalar_type,
    resolve_worker_pool_size,
    validate_variable_set_values,
)


class TestQueryParameters(unittest.TestCase):
    def test_resolve_worker_pool_size_auto(self):
        self.assertEqual(resolve_worker_pool_size(0, 100), AUTO_WORKER_POOL_SIZE)

    def test_resolve_worker_pool_size_unlimited(self):
        self.assertEqual(resolve_worker_pool_size(-1, 7), 7)

    def test_resolve_worker_pool_size_explicit(self):
        self.assertEqual(resolve_worker_pool_size(4, 100), 4)

    def test_resolve_worker_pool_size_rejects_invalid(self):
        with self.assertRaises(DbtRuntimeError):
            resolve_worker_pool_size(-2, 1)
        with self.assertRaises(DbtRuntimeError):
            resolve_worker_pool_size(True, 1)  # bool is not int
        with self.assertRaises(DbtRuntimeError):
            resolve_worker_pool_size("8", 1)
        with self.assertRaises(DbtRuntimeError):
            resolve_worker_pool_size(MAX_WORKER_POOL_SIZE + 1, 1)

    def test_infer_scalar_types(self):
        self.assertEqual(infer_scalar_type("a", True), "BOOL")
        self.assertEqual(infer_scalar_type("a", 1), "INT64")
        self.assertEqual(infer_scalar_type("a", 1.5), "FLOAT64")
        self.assertEqual(infer_scalar_type("a", "x"), "STRING")
        self.assertEqual(infer_scalar_type("a", datetime.date(2020, 1, 1)), "DATE")

    def test_infer_null_raises(self):
        with self.assertRaises(DbtRuntimeError):
            infer_scalar_type("a", None)

    def test_build_with_inference(self):
        params = build_query_parameters({"store_id": 1, "name": "a"})
        self.assertEqual(len(params), 2)
        self.assertIsInstance(params[0], ScalarQueryParameter)

    def test_build_with_explicit_types(self):
        params = build_query_parameters(
            {"store_id": 1},
            variable_set_types={"store_id": "INT64"},
        )
        self.assertEqual(params[0].name, "store_id")
        self.assertEqual(params[0].type_, "INT64")

    def test_null_with_explicit_type_raises(self):
        with self.assertRaises(DbtRuntimeError):
            build_query_parameters(
                {"store_id": None},
                variable_set_types={"store_id": "INT64"},
            )

    def test_array_type_reserved(self):
        with self.assertRaises(DbtRuntimeError) as ctx:
            build_query_parameters(
                {"ids": [1, 2]},
                variable_set_types={"ids": "ARRAY<INT64>"},
            )
        self.assertIn("ARRAY", str(ctx.exception))

    def test_struct_value_inference_reserved(self):
        with self.assertRaises(DbtRuntimeError) as ctx:
            build_query_parameters({"s": {"x": 1}})
        self.assertIn("STRUCT", str(ctx.exception))

    def test_validate_variable_set_values(self):
        self.assertEqual(validate_variable_set_values(None), [])
        self.assertEqual(validate_variable_set_values([{"a": 1}]), [{"a": 1}])
        with self.assertRaises(DbtRuntimeError):
            validate_variable_set_values("nope")
        with self.assertRaises(DbtRuntimeError):
            validate_variable_set_values([1])


class TestExecuteExt(unittest.TestCase):
    def setUp(self):
        self.credentials = Mock()
        self.credentials.method = "oauth"
        self.credentials.job_retries = 1
        self.credentials.job_retry_deadline_seconds = 1
        self.credentials.scopes = tuple()
        self.credentials.job_execution_timeout_seconds = 1
        self.credentials.priority = None
        self.credentials.maximum_bytes_billed = None
        self.credentials.reservation = None
        self.credentials.job_link_info_level_log = False

        self.connections = BigQueryConnectionManager(
            profile=Mock(credentials=self.credentials, query_comment=None),
            mp_context=Mock(),
        )
        # BaseConnectionManager.lock is used by get_if_exists(); keep it real.
        import threading

        self.connections.lock = threading.Lock()
        self.connections.thread_connections = {}

    def test_execute_ext_falls_back_to_execute(self):
        sentinel = (Mock(), Mock())
        with patch.object(self.connections, "execute", return_value=sentinel) as execute:
            result = self.connections.execute_ext("select 1")
        execute.assert_called_once()
        self.assertIs(result, sentinel)

    def test_execute_ext_empty_variable_sets(self):
        response, table = self.connections.execute_ext(
            "select 1", variable_set_values=[]
        )
        self.assertIn("0 parameterized", response._message)

    def test_execute_ext_parallel_success(self):
        mock_response = MagicMock()
        mock_response.bytes_processed = 10
        mock_response.bytes_billed = 10
        mock_response.slot_ms = 5
        mock_response.rows_affected = 1
        mock_response.job_id = "job"
        mock_response.location = "US"
        mock_response.project_id = "p"
        mock_response._message = "OK"

        def fake_raw_execute(sql, limit=None, query_parameters=None, on_attempt=None):
            if on_attempt:
                on_attempt(1)
            job = Mock()
            job.statement_type = "SELECT"
            job.total_bytes_processed = 10
            job.total_bytes_billed = 10
            job.slot_millis = 5
            job.location = "US"
            job.project = "p"
            job.job_id = "job"
            job.destination = "dest"
            return job, []

        with patch.object(self.connections, "set_connection_name"), patch.object(
            self.connections, "get_thread_connection", return_value=MagicMock()
        ), patch.object(self.connections, "release"), patch.object(
            self.connections, "raw_execute", side_effect=fake_raw_execute
        ), patch.object(
            self.connections,
            "_response_from_query_job",
            return_value=(mock_response, MagicMock()),
        ):
            response, _ = self.connections.execute_ext(
                "select @store_id",
                variable_set_values=[{"store_id": 1}, {"store_id": 2}],
                variable_set_types={"store_id": "INT64"},
                worker_pool_size=2,
            )

        self.assertEqual(response.code, "EXECUTE_EXT")
        self.assertIn("2 parameterized", response._message)

    def test_execute_ext_partial_failure_raises(self):
        calls = {"n": 0}

        def fake_raw_execute(sql, limit=None, query_parameters=None, on_attempt=None):
            calls["n"] += 1
            if calls["n"] == 1:
                raise RuntimeError("boom")
            job = Mock()
            return job, []

        mock_response = MagicMock(
            bytes_processed=1,
            bytes_billed=1,
            slot_ms=1,
            rows_affected=0,
            job_id="j",
            location="US",
            project_id="p",
            _message="OK",
        )

        with patch.object(self.connections, "set_connection_name"), patch.object(
            self.connections, "get_thread_connection", return_value=MagicMock()
        ), patch.object(self.connections, "release"), patch.object(
            self.connections, "raw_execute", side_effect=fake_raw_execute
        ), patch.object(
            self.connections,
            "_response_from_query_job",
            return_value=(mock_response, MagicMock()),
        ):
            with self.assertRaises(DbtRuntimeError) as ctx:
                self.connections.execute_ext(
                    "select @store_id",
                    variable_set_values=[{"store_id": 1}, {"store_id": 2}],
                    worker_pool_size=1,
                )
        self.assertIn("incomplete", str(ctx.exception))


if __name__ == "__main__":
    unittest.main()
