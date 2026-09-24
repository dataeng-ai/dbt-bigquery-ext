from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed, TimeoutError
from contextlib import contextmanager
from dataclasses import dataclass
import json
from multiprocessing.context import SpawnContext
import re
import time
from typing import (
    Any,
    Callable,
    Dict,
    Hashable,
    List,
    Mapping,
    Optional,
    Sequence,
    Tuple,
    TYPE_CHECKING,
)
import uuid

from google.auth.exceptions import RefreshError
from google.cloud.bigquery import (
    Client,
    CopyJobConfig,
    Dataset,
    DatasetReference,
    LoadJobConfig,
    QueryJobConfig,
    QueryPriority,
    SchemaField,
    Table,
    TableReference,
    WriteDisposition,
)
from google.cloud.exceptions import BadRequest, Conflict, Forbidden, NotFound

from dbt_common.events.contextvars import get_node_info
from dbt_common.events.functions import fire_event
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from dbt_common.invocation import get_invocation_id
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterRequiredConfig,
    AdapterResponse,
    ConnectionState,
)
from dbt.adapters.events.logging import AdapterLogger
from dbt.adapters.events.types import SQLQuery, SQLQueryStatus
from dbt.adapters.exceptions.connection import FailedToConnectError
from dbt.adapters.bigquery.clients import create_bigquery_client
from dbt.adapters.bigquery.credentials import Priority
from dbt.adapters.bigquery.query_parameters import (
    build_query_parameters,
    resolve_worker_pool_size,
    validate_variable_set_values,
)
from dbt.adapters.bigquery.retry import RetryFactory

if TYPE_CHECKING:
    # Indirectly imported via agate_helper, which is lazy loaded further downfile.
    # Used by mypy for earlier type hints.
    import agate


logger = AdapterLogger("BigQuery")


BQ_QUERY_JOB_SPLIT = "-----Query Job SQL Follows-----"


@dataclass
class BigQueryAdapterResponse(AdapterResponse):
    bytes_processed: Optional[int] = None
    bytes_billed: Optional[int] = None
    location: Optional[str] = None
    project_id: Optional[str] = None
    job_id: Optional[str] = None
    slot_ms: Optional[int] = None


class BigQueryConnectionManager(BaseConnectionManager):
    TYPE = "bigquery"

    def __init__(self, profile: AdapterRequiredConfig, mp_context: SpawnContext):
        super().__init__(profile, mp_context)
        self.jobs_by_thread: Dict[Hashable, List[str]] = defaultdict(list)
        self._retry = RetryFactory(profile.credentials)

    @classmethod
    def handle_error(cls, error, message):
        error_msg = "\n".join([item["message"] for item in error.errors])
        if hasattr(error, "query_job"):
            logger.error(
                cls._bq_job_link(
                    error.query_job.location, error.query_job.project, error.query_job.job_id
                )
            )
        raise DbtDatabaseError(error_msg)

    def clear_transaction(self):
        pass

    @contextmanager
    def exception_handler(self, sql):
        try:
            yield

        except BadRequest as e:
            message = "Bad request while running query"
            self.handle_error(e, message)

        except Forbidden as e:
            message = "Access denied while running query"
            self.handle_error(e, message)

        except NotFound as e:
            message = "Not found while running query"
            self.handle_error(e, message)

        except RefreshError as e:
            message = (
                "Unable to generate access token, if you're using "
                "impersonate_service_account, make sure your "
                'initial account has the "roles/'
                'iam.serviceAccountTokenCreator" role on the '
                "account you are trying to impersonate.\n\n"
                f"{str(e)}"
            )
            raise DbtDatabaseError(message)

        except Exception as e:
            logger.debug("Unhandled error while running:\n{}".format(sql))
            logger.debug(e)
            if isinstance(e, DbtRuntimeError):
                # during a sql query, an internal to dbt exception was raised.
                # this sounds a lot like a signal handler and probably has
                # useful information, so raise it without modification.
                raise
            exc_message = str(e)
            # the google bigquery library likes to add the query log, which we
            # don't want to log. Hopefully they never change this!
            if BQ_QUERY_JOB_SPLIT in exc_message:
                exc_message = exc_message.split(BQ_QUERY_JOB_SPLIT)[0].strip()
            raise DbtDatabaseError(exc_message)

    def cancel_open(self) -> List[str]:
        names = []
        this_connection = self.get_if_exists()
        with self.lock:
            for thread_id, connection in self.thread_connections.items():
                if connection is this_connection:
                    continue

                if connection.handle is not None and connection.state == ConnectionState.OPEN:
                    client: Client = connection.handle
                    for job_id in self.jobs_by_thread.get(thread_id, []):
                        with self.exception_handler(f"Cancel job: {job_id}"):
                            client.cancel_job(
                                job_id,
                                retry=self._retry.create_reopen_with_deadline(connection),
                            )
                    self.close(connection)

                if connection.name is not None:
                    names.append(connection.name)
        return names

    @classmethod
    def close(cls, connection):
        connection.handle.close()
        connection.state = ConnectionState.CLOSED

        return connection

    def begin(self):
        pass

    def commit(self):
        pass

    def format_bytes(self, num_bytes):
        if num_bytes:
            for unit in ["Bytes", "KiB", "MiB", "GiB", "TiB", "PiB"]:
                if abs(num_bytes) < 1024.0:
                    return f"{num_bytes:3.1f} {unit}"
                num_bytes /= 1024.0

            num_bytes *= 1024.0
            return f"{num_bytes:3.1f} {unit}"

        else:
            return num_bytes

    def format_rows_number(self, rows_number):
        for unit in ["", "k", "m", "b", "t"]:
            if abs(rows_number) < 1000.0:
                return f"{rows_number:3.1f}{unit}".strip()
            rows_number /= 1000.0

        rows_number *= 1000.0
        return f"{rows_number:3.1f}{unit}".strip()

    @classmethod
    def open(cls, connection):
        if connection.state == ConnectionState.OPEN:
            logger.debug("Connection is already open, skipping open.")
            return connection

        try:
            connection.handle = create_bigquery_client(connection.credentials)
            connection.state = ConnectionState.OPEN
            return connection

        except Exception as e:
            logger.debug(f"""Got an error when attempting to create a bigquery " "client: '{e}'""")
            connection.handle = None
            connection.state = ConnectionState.FAIL
            raise FailedToConnectError(str(e))

    @classmethod
    def get_table_from_response(cls, resp) -> "agate.Table":
        from dbt_common.clients import agate_helper

        column_names = [field.name for field in resp.schema]
        return agate_helper.table_from_data_flat(resp, column_names)

    def get_labels_from_query_comment(cls):
        if (
            hasattr(cls.profile, "query_comment")
            and cls.profile.query_comment
            and cls.profile.query_comment.job_label
            and cls.query_header
        ):
            query_comment = cls.query_header.comment.query_comment
            return cls._labels_from_query_comment(query_comment)

        return {}

    def generate_job_id(self) -> str:
        # Generating a fresh job_id for every _query_and_results call to avoid job_id reuse.
        # Generating a job id instead of persisting a BigQuery-generated one after client.query is called.
        # Using BigQuery's job_id can lead to a race condition if a job has been started and a termination
        # is sent before the job_id was stored, leading to a failure to cancel the job.
        # By predetermining job_ids (uuid4), we can persist the job_id before the job has been kicked off.
        # Doing this, the race condition only leads to attempting to cancel a job that doesn't exist.
        job_id = str(uuid.uuid4())
        thread_id = self.get_thread_identifier()
        self.jobs_by_thread[thread_id].append(job_id)
        return job_id

    def _submit_or_attach(self, client: Client, job_id: str, submit: Callable):
        """Submit a job, or attach to the existing one via get_job on 409 Conflict.

        A stable job_id makes jobs.insert idempotent: a resubmission (dbt's retry
        or the client library's transport retry after a lost response) attaches to
        the in-flight job instead of spawning a second one that re-runs work.
        """
        try:
            return submit()
        except Conflict:
            logger.debug(
                f"Job {job_id} already exists; attaching to the in-flight job "
                "instead of resubmitting to avoid duplicate execution."
            )
            return client.get_job(job_id)

    def raw_execute(
        self,
        sql,
        use_legacy_sql=False,
        limit: Optional[int] = None,
        dry_run: bool = False,
        query_parameters: Optional[List[Any]] = None,
        on_attempt: Optional[Callable[[int], None]] = None,
    ):
        conn = self.get_thread_connection()

        fire_event(SQLQuery(conn_name=conn.name, sql=sql, node_info=get_node_info()))

        labels = self.get_labels_from_query_comment()

        labels["dbt_invocation_id"] = get_invocation_id()

        # Parameterized queries require GoogleSQL.
        if query_parameters:
            use_legacy_sql = False

        job_params = {
            "use_legacy_sql": use_legacy_sql,
            "labels": labels,
            "dry_run": dry_run,
        }

        if query_parameters:
            job_params["query_parameters"] = query_parameters

        priority = conn.credentials.priority
        if priority == Priority.Batch:
            job_params["priority"] = QueryPriority.BATCH
        else:
            job_params["priority"] = QueryPriority.INTERACTIVE

        maximum_bytes_billed = conn.credentials.maximum_bytes_billed
        if maximum_bytes_billed is not None and maximum_bytes_billed != 0:
            job_params["maximum_bytes_billed"] = maximum_bytes_billed

        model_reservation = getattr(conn, "_bq_model_reservation", None)
        reservation = (
            model_reservation if model_reservation is not None else conn.credentials.reservation
        )

        if reservation is not None:
            job_params["reservation"] = reservation

        model_timeout = getattr(conn, "_bq_model_timeout", None)
        if model_timeout is not None:
            job_params["job_timeout_ms"] = int(model_timeout * 1000)

        with self.exception_handler(sql):
            # Mint the job_id once, outside the retry closure, so a re-entry
            # resubmits the same job instead of spawning a duplicate.
            job_id = self.generate_job_id()
            attempt_num = {"n": 0}

            def _execute_with_retry():
                attempt_num["n"] += 1
                if on_attempt is not None:
                    on_attempt(attempt_num["n"])
                # Copy job_params so timeout mutation in _query_and_results
                # does not leak across retry attempts / parallel workers.
                return self._query_and_results(
                    conn,
                    sql,
                    dict(job_params),
                    job_id,
                    limit=limit,
                )

            retry = self._retry.create_reopen_with_deadline(conn)
            return retry(_execute_with_retry)()

    def raw_execute_with_comment(
        self,
        sql: str,
        use_legacy_sql: bool = False,
        limit: Optional[int] = None,
        dry_run: bool = False,
    ):
        """
        A lightweight wrapper over raw_execute that prepends the dbt query comment.

        This exists as a "third way" between raw_execute (fully manual, no preprocessing)
        and execute (postprocessing and formatting). This is useful when you need query
        auditing but no Adapter Response.
        """
        sql = self._add_query_comment(sql)
        return self.raw_execute(
            sql,
            use_legacy_sql=use_legacy_sql,
            limit=limit,
            dry_run=dry_run,
        )

    def execute(
        self, sql, auto_begin=False, fetch=None, limit: Optional[int] = None
    ) -> Tuple[BigQueryAdapterResponse, "agate.Table"]:
        sql = self._add_query_comment(sql)
        # auto_begin is ignored on bigquery, and only included for consistency
        query_job, iterator = self.raw_execute(sql, limit=limit)

        if fetch:
            table = self.get_table_from_response(iterator)
        else:
            from dbt_common.clients import agate_helper

            table = agate_helper.empty_table()

        message = "OK"
        code = None
        num_rows = None
        bytes_processed = None
        bytes_billed = None
        location = None
        job_id = None
        project_id = None
        num_rows_formatted = None
        processed_bytes = None
        slot_ms = None

        if query_job.statement_type == "CREATE_VIEW":
            code = "CREATE VIEW"

        elif query_job.statement_type == "CREATE_TABLE_AS_SELECT":
            code = "CREATE TABLE"
            conn = self.get_thread_connection()
            client = conn.handle
            query_table = client.get_table(query_job.destination)
            num_rows = query_table.num_rows

        elif query_job.statement_type == "SCRIPT":
            code = "SCRIPT"

        elif query_job.statement_type in ["INSERT", "DELETE", "MERGE", "UPDATE"]:
            code = query_job.statement_type
            num_rows = query_job.num_dml_affected_rows

        elif query_job.statement_type == "SELECT":
            code = "SELECT"
            conn = self.get_thread_connection()
            client = conn.handle
            # use anonymous table for num_rows
            query_table = client.get_table(query_job.destination)
            num_rows = query_table.num_rows

        # set common attributes
        bytes_processed = query_job.total_bytes_processed
        bytes_billed = query_job.total_bytes_billed
        slot_ms = query_job.slot_millis
        processed_bytes = self.format_bytes(bytes_processed)
        location = query_job.location
        job_id = query_job.job_id
        project_id = query_job.project
        if num_rows is not None:
            num_rows_formatted = self.format_rows_number(num_rows)
            message = f"{code} ({num_rows_formatted} rows, {processed_bytes} processed)"
        elif bytes_processed is not None:
            message = f"{code} ({processed_bytes} processed)"
        else:
            message = f"{code}"

        response = BigQueryAdapterResponse(
            _message=message,
            rows_affected=num_rows,
            code=code,
            bytes_processed=bytes_processed,
            bytes_billed=bytes_billed,
            location=location,
            project_id=project_id,
            job_id=job_id,
            slot_ms=slot_ms,
        )

        return response, table

    def load_variable_sets_from_relation(
        self, relation: Any
    ) -> Tuple[List[Mapping[str, Any]], Dict[str, str]]:
        """``SELECT *`` from ``relation`` → (variable_set_values, variable_set_types)."""
        from dbt.adapters.bigquery.query_parameters import (
            render_variable_set_relation,
            variable_sets_from_bq_rows,
        )

        rendered = render_variable_set_relation(relation)
        sql = f"select * from {rendered}"
        logger.debug(f"execute_ext: loading variable sets from relation {rendered}")
        _, iterator = self.raw_execute(sql, limit=None)
        rows = list(iterator)
        schema = getattr(iterator, "schema", None)
        values, types = variable_sets_from_bq_rows(rows, schema)
        logger.debug(
            f"execute_ext: loaded {len(values)} variable set(s) "
            f"with columns {list(types.keys())} from {rendered}"
        )
        return values, types

    def resolve_execute_ext_variable_sets(
        self,
        variable_set_values: Optional[Sequence[Mapping[str, Any]]] = None,
        variable_set_types: Optional[Mapping[str, str]] = None,
        variable_set_relation: Any = None,
    ) -> Tuple[Optional[List[Mapping[str, Any]]], Optional[Dict[str, str]]]:
        """Resolve explicit values or a relation into (values, types).

        Returns ``(None, None)`` when neither source is set (caller should fall
        back to a single ``execute``).
        """
        from dbt.adapters.bigquery.query_parameters import validate_variable_set_source

        validate_variable_set_source(
            variable_set_values, variable_set_relation, variable_set_types
        )
        if variable_set_relation is not None:
            values, types = self.load_variable_sets_from_relation(variable_set_relation)
            return list(values), dict(types)
        if variable_set_values is None:
            return None, None
        return validate_variable_set_values(variable_set_values), (
            dict(variable_set_types) if variable_set_types is not None else None
        )

    def execute_ext(
        self,
        sql,
        auto_begin=False,
        fetch=None,
        limit: Optional[int] = None,
        variable_set_values: Optional[Sequence[Mapping[str, Any]]] = None,
        worker_pool_size: int = 0,
        variable_set_types: Optional[Mapping[str, str]] = None,
        variable_set_relation: Any = None,
    ) -> Tuple[BigQueryAdapterResponse, "agate.Table"]:
        """Execute SQL once, or many times in parallel with query parameters.

        Pass **either** ``variable_set_values`` (+ optional types) **or**
        ``variable_set_relation`` (types from the relation schema). When neither
        is set, behaves like ``execute``.

        worker_pool_size:
          * 0  — auto parallelism (16 workers)
          * -1 — one worker per variable set (unlimited relative to the batch)
          * >0 — explicit pool size

        Partial failures are not rolled back. If any job fails after all have
        finished, a ``DbtRuntimeError`` is raised (overall status = failed).
        Callers must use idempotent SQL or handle partial runs outside dbt.
        """
        from dbt_common.clients import agate_helper

        variable_sets, variable_set_types = self.resolve_execute_ext_variable_sets(
            variable_set_values=variable_set_values,
            variable_set_types=variable_set_types,
            variable_set_relation=variable_set_relation,
        )

        # Fallback: no parameterized batch → regular execute.
        if variable_sets is None:
            return self.execute(sql, auto_begin=auto_begin, fetch=fetch, limit=limit)

        sql = self._add_query_comment(sql)

        if not variable_sets:
            logger.debug("execute_ext: variable_set_values is empty; nothing to run")
            return (
                BigQueryAdapterResponse(_message="OK (0 parameterized queries)"),
                agate_helper.empty_table(),
            )

        pool_size = resolve_worker_pool_size(worker_pool_size, len(variable_sets))
        logger.debug(
            f"execute_ext: starting {len(variable_sets)} parameterized quer"
            f"{'y' if len(variable_sets) == 1 else 'ies'} "
            f"with worker_pool_size={worker_pool_size} (resolved={pool_size})"
        )

        # Preserve parent-thread connection attrs (timeout / reservation) for workers.
        parent_conn = self.get_if_exists()
        parent_timeout = getattr(parent_conn, "_bq_model_timeout", None) if parent_conn else None
        parent_reservation = (
            getattr(parent_conn, "_bq_model_reservation", None) if parent_conn else None
        )

        results: Dict[int, Tuple[BigQueryAdapterResponse, Any]] = {}
        errors: Dict[int, BaseException] = {}

        def _run_one(index: int, var_set: Mapping[str, Any]):
            conn_name = f"execute_ext_{index}"
            self.set_connection_name(conn_name)
            conn = self.get_thread_connection()
            if parent_timeout is not None:
                conn._bq_model_timeout = parent_timeout
            if parent_reservation is not None:
                conn._bq_model_reservation = parent_reservation

            logger.debug(
                f"execute_ext: starting execution for var set [{index}]: {dict(var_set)}"
            )
            attempt_state = {"last": 0}

            def _on_attempt(n: int) -> None:
                if n > 1:
                    logger.debug(
                        f"execute_ext: retried var set [{index}] "
                        f"(attempt {n}) params={dict(var_set)}"
                    )
                attempt_state["last"] = n

            try:
                query_parameters = build_query_parameters(var_set, variable_set_types)
                query_job, iterator = self.raw_execute(
                    sql,
                    limit=limit,
                    query_parameters=query_parameters,
                    on_attempt=_on_attempt,
                )
                response, table = self._response_from_query_job(
                    query_job, iterator, fetch=bool(fetch)
                )
                logger.debug(
                    f"execute_ext: completed var set [{index}] "
                    f"job_id={response.job_id} message={response._message}"
                )
                return index, response, table, None
            except BaseException as exc:
                logger.debug(f"execute_ext: failed var set [{index}]: {exc}")
                return index, None, None, exc
            finally:
                self.release()

        with ThreadPoolExecutor(max_workers=pool_size) as executor:
            futures = [
                executor.submit(_run_one, idx, var_set)
                for idx, var_set in enumerate(variable_sets)
            ]
            for future in as_completed(futures):
                index, response, table, exc = future.result()
                if exc is not None:
                    errors[index] = exc
                else:
                    results[index] = (response, table)

        succeeded = len(results)
        failed = len(errors)
        logger.debug(
            f"execute_ext: finished batch — succeeded={succeeded} "
            f"failed={failed} total={len(variable_sets)}"
        )

        if errors:
            details = "; ".join(
                f"[{idx}] {errors[idx]}" for idx in sorted(errors)
            )
            raise DbtRuntimeError(
                f"execute_ext incomplete: {failed}/{len(variable_sets)} queries failed "
                f"(partial runs are not rolled back). Failures: {details}"
            )

        # Aggregate responses; multi-query fetch returns an empty table.
        ordered = [results[i][0] for i in sorted(results)]
        total_bytes = sum(r.bytes_processed or 0 for r in ordered)
        total_billed = sum(r.bytes_billed or 0 for r in ordered)
        total_slots = sum(r.slot_ms or 0 for r in ordered)
        total_rows = sum(r.rows_affected or 0 for r in ordered if r.rows_affected is not None)
        job_ids = ",".join(r.job_id for r in ordered if r.job_id)

        aggregated = BigQueryAdapterResponse(
            _message=f"OK ({succeeded} parameterized queries)",
            rows_affected=total_rows if total_rows else None,
            code="EXECUTE_EXT",
            bytes_processed=total_bytes or None,
            bytes_billed=total_billed or None,
            location=ordered[0].location if ordered else None,
            project_id=ordered[0].project_id if ordered else None,
            job_id=job_ids or None,
            slot_ms=total_slots or None,
        )
        return aggregated, agate_helper.empty_table()

    def _response_from_query_job(
        self, query_job, iterator, fetch: bool = False
    ) -> Tuple[BigQueryAdapterResponse, "agate.Table"]:
        """Build an AdapterResponse (+ optional agate table) from a finished QueryJob."""
        from dbt_common.clients import agate_helper

        if fetch:
            table = self.get_table_from_response(iterator)
        else:
            table = agate_helper.empty_table()

        message = "OK"
        code = None
        num_rows = None

        if query_job.statement_type == "CREATE_VIEW":
            code = "CREATE VIEW"
        elif query_job.statement_type == "CREATE_TABLE_AS_SELECT":
            code = "CREATE TABLE"
            conn = self.get_thread_connection()
            client = conn.handle
            query_table = client.get_table(query_job.destination)
            num_rows = query_table.num_rows
        elif query_job.statement_type == "SCRIPT":
            code = "SCRIPT"
        elif query_job.statement_type in ["INSERT", "DELETE", "MERGE", "UPDATE"]:
            code = query_job.statement_type
            num_rows = query_job.num_dml_affected_rows
        elif query_job.statement_type == "SELECT":
            code = "SELECT"
            conn = self.get_thread_connection()
            client = conn.handle
            query_table = client.get_table(query_job.destination)
            num_rows = query_table.num_rows

        bytes_processed = query_job.total_bytes_processed
        bytes_billed = query_job.total_bytes_billed
        slot_ms = query_job.slot_millis
        processed_bytes = self.format_bytes(bytes_processed)
        if num_rows is not None:
            num_rows_formatted = self.format_rows_number(num_rows)
            message = f"{code} ({num_rows_formatted} rows, {processed_bytes} processed)"
        elif bytes_processed is not None:
            message = f"{code} ({processed_bytes} processed)"
        else:
            message = f"{code}"

        response = BigQueryAdapterResponse(
            _message=message,
            rows_affected=num_rows,
            code=code,
            bytes_processed=bytes_processed,
            bytes_billed=bytes_billed,
            location=query_job.location,
            project_id=query_job.project,
            job_id=query_job.job_id,
            slot_ms=slot_ms,
        )
        return response, table

    def dry_run(self, sql: str) -> BigQueryAdapterResponse:
        """Run the given sql statement with the `dry_run` job parameter set.

        This will allow BigQuery to validate the SQL and immediately return job cost
        estimates, which we capture in the BigQueryAdapterResponse. Invalid SQL
        will result in an exception.
        """
        sql = self._add_query_comment(sql)
        query_job, _ = self.raw_execute(sql, dry_run=True)

        # TODO: Factor this repetitive block out into a factory method on
        # BigQueryAdapterResponse
        message = f"Ran dry run query for statement of type {query_job.statement_type}"
        bytes_billed = query_job.total_bytes_billed
        processed_bytes = self.format_bytes(query_job.total_bytes_processed)
        location = query_job.location
        project_id = query_job.project
        job_id = query_job.job_id
        slot_ms = query_job.slot_millis

        return BigQueryAdapterResponse(
            _message=message,
            code="DRY RUN",
            bytes_billed=bytes_billed,
            bytes_processed=processed_bytes,
            location=location,
            project_id=project_id,
            job_id=job_id,
            slot_ms=slot_ms,
        )

    @staticmethod
    def _bq_job_link(location, project_id, job_id) -> str:
        return f"https://console.cloud.google.com/bigquery?project={project_id}&j=bq:{location}:{job_id}&page=queryresults"

    def get_partitions_metadata(self, table):
        if getattr(self, "use_standard_sql_for_partitions", False):
            sql = f"""
                SELECT partition_id
                FROM `{table.project}.{table.dataset}.INFORMATION_SCHEMA.PARTITIONS`
                WHERE table_name = '{table.identifier}'
            """
            sql = self._add_query_comment(sql)
            _, iterator = self.raw_execute(sql, use_legacy_sql=False)
        else:

            def standard_to_legacy(table):
                return table.project + ":" + table.dataset + "." + table.identifier

            legacy_sql = "SELECT * FROM [" + standard_to_legacy(table) + "$__PARTITIONS_SUMMARY__]"
            sql = self._add_query_comment(legacy_sql)
            _, iterator = self.raw_execute(sql, use_legacy_sql=True)

        return self.get_table_from_response(iterator)

    def copy_bq_table(self, source, destination, write_disposition) -> None:
        conn = self.get_thread_connection()
        client: Client = conn.handle

        # -------------------------------------------------------------------------------
        #  BigQuery allows to use copy API using two different formats:
        #  1. client.copy_table(source_table_id, destination_table_id)
        #     where source_table_id = "your-project.source_dataset.source_table"
        #  2. client.copy_table(source_table_ids, destination_table_id)
        #     where source_table_ids = ["your-project.your_dataset.your_table_name", ...]
        #  Let's use uniform function call and always pass list there
        # -------------------------------------------------------------------------------
        if type(source) is not list:
            source = [source]

        source_ref_array = [
            self.table_ref(src_table.database, src_table.schema, src_table.table)
            for src_table in source
        ]
        destination_ref = self.table_ref(
            destination.database, destination.schema, destination.table
        )

        logger.debug(
            'Copying table(s) "{}" to "{}" with disposition: "{}"',
            ", ".join(source_ref.path for source_ref in source_ref_array),
            destination_ref.path,
            write_disposition,
        )

        msg = 'copy table "{}" to "{}"'.format(
            ", ".join(source_ref.path for source_ref in source_ref_array),
            destination_ref.path,
        )
        with self.exception_handler(msg):
            # Stable job_id: copy_table has no built-in 409 recovery of its own.
            job_id = self.generate_job_id()
            copy_job = self._submit_or_attach(
                client,
                job_id,
                lambda: client.copy_table(
                    source_ref_array,
                    destination_ref,
                    job_config=CopyJobConfig(write_disposition=write_disposition),
                    job_id=job_id,
                    retry=self._retry.create_reopen_with_deadline(conn),
                ),
            )
            model_timeout = getattr(conn, "_bq_model_timeout", None)
            copy_timeout = model_timeout or self._retry.create_job_execution_timeout(fallback=300)
            copy_job.result(timeout=copy_timeout)

    def write_dataframe_to_table(
        self,
        client: Client,
        file_path: str,
        database: str,
        schema: str,
        identifier: str,
        table_schema: List[SchemaField],
        field_delimiter: str,
        fallback_timeout: Optional[float] = None,
    ) -> None:
        load_config = LoadJobConfig(
            skip_leading_rows=1,
            schema=table_schema,
            field_delimiter=field_delimiter,
            # Seeds always fully replace the target; don't rely on BigQuery's
            # job-level default (WRITE_APPEND) if the caller's drop was skipped.
            write_disposition=WriteDisposition.WRITE_TRUNCATE,
        )
        table = self.table_ref(database, schema, identifier)
        self._write_file_to_table(client, file_path, table, load_config, fallback_timeout)

    def write_file_to_table(
        self,
        client: Client,
        file_path: str,
        database: str,
        schema: str,
        identifier: str,
        fallback_timeout: Optional[float] = None,
        **kwargs,
    ) -> None:
        config = kwargs["kwargs"]
        if "schema" in config:
            config["schema"] = json.load(config["schema"])
        load_config = LoadJobConfig(**config)
        table = self.table_ref(database, schema, identifier)
        self._write_file_to_table(client, file_path, table, load_config, fallback_timeout)

    def _write_file_to_table(
        self,
        client: Client,
        file_path: str,
        table: TableReference,
        config: LoadJobConfig,
        fallback_timeout: Optional[float] = None,
    ) -> None:

        with self.exception_handler("LOAD TABLE"):
            with open(file_path, "rb") as f:
                job = client.load_table_from_file(f, table, rewind=True, job_config=config)

        response = job.result(retry=self._retry.create_retry(fallback=fallback_timeout))

        if response.state != "DONE":
            raise DbtDatabaseError("BigQuery Timeout Exceeded")

        elif response.error_result:
            message = "\n".join(error["message"].strip() for error in response.errors)
            raise DbtDatabaseError(message)

    @staticmethod
    def dataset_ref(database, schema):
        return DatasetReference(project=database, dataset_id=schema)

    @staticmethod
    def table_ref(database, schema, table_name):
        dataset_ref = DatasetReference(database, schema)
        return TableReference(dataset_ref, table_name)

    def get_bq_table(self, database, schema, identifier) -> Table:
        """Get a bigquery table for a schema/model."""
        conn = self.get_thread_connection()
        client: Client = conn.handle
        # backwards compatibility: fill in with defaults if not specified
        database = database or conn.credentials.database
        schema = schema or conn.credentials.schema
        return client.get_table(self.table_ref(database, schema, identifier))

    def drop_dataset(self, database, schema) -> None:
        conn = self.get_thread_connection()
        client: Client = conn.handle
        with self.exception_handler("drop dataset"):
            client.delete_dataset(
                dataset=self.dataset_ref(database, schema),
                delete_contents=True,
                not_found_ok=True,
                retry=self._retry.create_reopen_with_deadline(conn),
            )

    def create_dataset(self, database, schema) -> Dataset:
        conn = self.get_thread_connection()
        client: Client = conn.handle
        with self.exception_handler("create dataset"):
            return client.create_dataset(
                dataset=self.dataset_ref(database, schema),
                exists_ok=True,
                retry=self._retry.create_reopen_with_deadline(conn),
            )

    def list_dataset(self, database: str):
        # The database string we get here is potentially quoted.
        # Strip that off for the API call.
        conn = self.get_thread_connection()
        client: Client = conn.handle
        with self.exception_handler("list dataset"):
            # this is similar to how we have to deal with listing tables
            all_datasets = client.list_datasets(
                project=database.strip("`"),
                max_results=10000,
                retry=self._retry.create_reopen_with_deadline(conn),
            )
            return [ds.dataset_id for ds in all_datasets]

    def _query_and_results(
        self,
        conn,
        sql,
        job_params,
        job_id,
        limit: Optional[int] = None,
    ):
        """Query the client and wait for results."""
        client: Client = conn.handle
        # Only set job_timeout_ms from profile if not already set (e.g., via model-level config)
        if "job_timeout_ms" in job_params:
            timeout = job_params["job_timeout_ms"] / 1000
        else:
            timeout = self._retry.create_job_execution_timeout()
            if timeout:
                job_params["job_timeout_ms"] = int(timeout * 1000)
        query_job_config = QueryJobConfig(**job_params)
        polling_timeout = (
            timeout + 30 if timeout else None
        )  # buffer for polling after job execution timeout
        # Cannot reuse job_config if destination is set and ddl is used.
        # job_id is stable across retries (see raw_execute).
        query_job = self._submit_or_attach(
            client,
            job_id,
            lambda: client.query(
                query=sql,
                job_config=query_job_config,
                job_id=job_id,
                job_retry=None,
                timeout=self._retry.create_job_creation_timeout(),
            ),
        )
        if (
            query_job.location is not None
            and query_job.job_id is not None
            and query_job.project is not None
        ):
            job_link = self._bq_job_link(query_job.location, query_job.project, query_job.job_id)
            if conn.credentials.job_link_info_level_log:
                logger.info(job_link)
            else:
                logger.debug(job_link)

        pre = time.perf_counter()
        try:
            iterator = query_job.result(
                max_results=limit,
                timeout=polling_timeout,
                retry=self._retry.create_query_job_polling_retry(query_job),
            )
        except TimeoutError:
            exc = f"Operation did not complete within the designated timeout of {timeout} seconds."
            try:
                query_job.cancel()
            except Exception as e:
                logger.debug(f"Error cancelling query job: {e}")
            raise TimeoutError(exc)

        fire_event(
            SQLQueryStatus(
                status="OK",
                elapsed=time.perf_counter() - pre,
                node_info=get_node_info(),
                query_id=query_job.job_id,
            )
        )

        return query_job, iterator

    def _labels_from_query_comment(self, comment: str) -> Dict:
        try:
            comment_labels = json.loads(comment)
        except (TypeError, ValueError):
            return {"query_comment": _sanitize_label(comment)}
        return {
            _sanitize_label(key): _sanitize_label(str(value))
            for key, value in comment_labels.items()
        }


_SANITIZE_LABEL_PATTERN = re.compile(r"[^a-z0-9_-]")

_VALIDATE_LABEL_LENGTH_LIMIT = 63


def _sanitize_label(value: str) -> str:
    """Return a legal value for a BigQuery label."""
    value = value.strip().lower()
    value = _SANITIZE_LABEL_PATTERN.sub("_", value)
    return value[:_VALIDATE_LABEL_LENGTH_LIMIT]
