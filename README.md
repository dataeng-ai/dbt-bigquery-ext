# dbt-bigquery-ext

DataEng AI fork of [`dbt-bigquery`](https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-bigquery)
(based on **1.12.1**), published as a **drop-in replacement**.

- **PyPI:** `dbt-bigquery-ext`
- **Import / adapter type:** still `dbt.adapters.bigquery` / `type: bigquery` in profiles
- **Do not install alongside** upstream `dbt-bigquery` (same Python package path)

## Install

```bash
pip uninstall -y dbt-bigquery
pip install dbt-bigquery-ext
```

## execute_ext

Runs the model's main SQL once per variable set, in parallel, as a [BigQuery parameterized query](https://docs.cloud.google.com/bigquery/docs/parameterized-queries) (`@name` placeholders). The adapter overrides the core `statement` macro and, when `config.execute_ext` is set, sends only the `main` statement through `adapter.execute_ext`.

`execute_ext` is allowed only on materializations `incremental_ext` and `script`. Any other materialization raises.

Without `execute_ext` config, behavior matches upstream `dbt-bigquery`. `incremental_ext` and `script` are still available and run their main SQL once.

### incremental_ext

Incremental materialization for parameterized fan-out via `execute_ext`. It does not `CREATE OR REPLACE` the target and it does not build a shared dataset `__dbt_tmp`.

Serial, once per run:

1. On `--full-refresh`, `DROP` the existing relation. Truncate would keep the old partition and cluster spec and fail when those change.
2. `CREATE TABLE IF NOT EXISTS <target> AS SELECT * FROM (<model>) WHERE FALSE` so a missing table is created empty, with the current partition and cluster config.
3. When `on_schema_change` is not `ignore`, create a dataset temp the same empty way, apply the schema change to the target, then drop the temp.

`incremental_strategy` is `merge` (default) or `insert_overwrite`.

#### merge (default)

The `main` statement is one BigQuery script per variable set:

```sql
BEGIN
  DECLARE _dbt_merge_partition_dates ARRAY<DATE>;

  CREATE TEMP TABLE _dbt_ext_src AS (
    <model sql, with @parameters>
  );

  SET _dbt_merge_partition_dates = (
    SELECT ARRAY_AGG(dt) FROM (
      SELECT DISTINCT DATE(<partition_field>) AS dt FROM _dbt_ext_src
      WHERE <partition_field> IS NOT NULL
    )
  );

  MERGE INTO <target> ... USING (SELECT * FROM _dbt_ext_src) ...
    ON (...keys...)
   AND DATE(DBT_INTERNAL_DEST.<partition_field>) IN UNNEST(
         IFNULL(_dbt_merge_partition_dates, [DATE '1900-01-02'])
       );
END;
```

`CREATE TEMP TABLE` lives in that script's job, so concurrent `execute_ext` workers do not see each other's temp tables. Each merge writes only its own rows into the shared target. `unique_key` is optional: with it, matched rows update; without it, the merge is insert-only (append), same as regular `incremental` + `merge`.

**Partition predicates (always on for time-partitioned merge):** both `incremental` and `incremental_ext` load distinct `DATE(<partition_field>)` values from the staging relation into a script variable and filter with `IN UNNEST(...)` (verified to partition-prune on BigQuery). Regular `incremental` always builds `__dbt_tmp` for time-partitioned merge (even when `on_schema_change='ignore'`) so the MERGE script can `SET` the variable from it. Applies when `partition_by.data_type` is `date` / `timestamp` / `datetime` — no config flag.

#### merge_skip_unchanged

On `incremental` and `incremental_ext` with `incremental_strategy='merge'` and a `unique_key`, skip no-op updates when matched row data is unchanged:

```sql
WHEN MATCHED
  AND (
    STRUCT(DBT_INTERNAL_SOURCE.`name`, DBT_INTERNAL_SOURCE.`status`)
    IS DISTINCT FROM
    STRUCT(DBT_INTERNAL_DEST.`name`, DBT_INTERNAL_DEST.`status`)
  )
THEN UPDATE SET ...
```

Uses BigQuery `STRUCT(...) IS DISTINCT FROM STRUCT(...)` (null-safe; no JSON/hash). Compare columns must be groupable (not `GEOGRAPHY` / `JSON`, etc.).

```sql
{{ config(
    materialized="incremental",  -- or incremental_ext
    incremental_strategy="merge",
    unique_key="id",
    merge_skip_unchanged=true,
    -- XOR: at most one of:
    -- stable_columns=["name", "status"],           -- compare only these
    -- unstable_columns=["__ingestion_time"],       -- compare all except these
    -- neither → compare all target columns (minus unique_key)
) }}
```

- Requires `unique_key`; rejected on `insert_overwrite` / `microbatch`
- `unique_key` columns are omitted from the compare set automatically
- Independent of `merge_update_columns` / `merge_exclude_columns` (those control the `SET` list)
- First run / full refresh: no matched rows (or CTAS) → no-op for this predicate
- Schema append: compare uses current `dest_columns` after schema sync

#### insert_overwrite

Same staging temp as merge, then DELETE + MERGE in a
[transaction](https://docs.cloud.google.com/bigquery/docs/transactions):

```sql
BEGIN
  DECLARE _dbt_merge_partition_dates ARRAY<DATE>;

  CREATE TEMP TABLE _dbt_ext_src AS (
    <model sql, with @dt>
  );

  BEGIN TRANSACTION;

  DELETE FROM <target>
  WHERE DATETIME_TRUNC(CAST(dt AS DATETIME), DAY) = DATETIME_TRUNC(CAST(@dt AS DATETIME), DAY);  -- shape depends on data_type/granularity

  MERGE INTO <target> ...
  USING (
    SELECT * FROM _dbt_ext_src
    WHERE DATETIME_TRUNC(CAST(dt AS DATETIME), DAY) = DATETIME_TRUNC(CAST(@dt AS DATETIME), DAY)
  ) ...
  ;

  COMMIT TRANSACTION;
END;
```

If MERGE fails after DELETE, BigQuery rolls the transaction back so the partition is not left empty.

(Exact predicate uses `{DATA_TYPE}_TRUNC(CAST(col AS …), GRAN)` when needed — e.g. `TIMESTAMP_TRUNC(..., HOUR)` — or `CAST(col AS DATE) = CAST(@param AS DATE)` for `date` + `day`. Both sides are cast so column storage type need not match `partition_by.data_type` exactly.)

This keeps temp creation identical to merge and applies the partition guard on the MERGE source (same place merge already reads `SELECT * FROM _dbt_ext_src`).

Requirements:

- Time partitioning only: `partition_by.data_type` in `date` / `timestamp` / `datetime`
- Granularity `hour` / `day` / `month` / `year` (`hour` requires timestamp/datetime)
- `execute_ext` required; each variable set has **exactly one** parameter (name may differ from `partition_by.field`, e.g. `@dt` vs column `__taken_at_utc`)
- Parameter type `DATE` / `STRING` / `TIMESTAMP` / `DATETIME`; values coerce to the partition type. For `hour`, pass a timestamp/datetime (not date-only)
- Variable sets are **deduped by partition bucket** (first wins) so duplicate days/hours do not race
- `unique_key` is not allowed
- `copy_partitions` is not supported (see below)

`copy_partitions` (upstream dbt-bigquery): when true, regular `incremental` + `insert_overwrite` can replace partitions by copying whole partition shards (`table$YYYYMMDD`) via the BigQuery Jobs API instead of row SQL. **`incremental_ext` does not implement that path** — it always runs `DELETE` of the matching bucket then insert-only `MERGE`. Passing `copy_partitions: true` raises.

```sql
{{ config(
    materialized="incremental_ext",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "__taken_at_utc", "data_type": "datetime", "granularity": "day"},
    execute_ext={
        "variable_set_sql": "SELECT dt FROM UNNEST([DATE '2026-09-01', DATE '2026-09-03']) AS dt",
        "worker_pool_size": 0,
    },
) }}

select
  @dt as dt,
  cast(@dt as timestamp) as __taken_at_utc
```

DELETE/MERGE compare `CAST(<partition_by.field> AS <data_type>)` to `CAST(@<sole_param> AS <data_type>)` (with `_TRUNC` when needed), so the parameter name and physical column type need not match `partition_by` exactly.

## Relation markers (`ref` / `source`)

When a model sets `mark_relations=true`, or uses materialization `incremental_ext` / `script`, overridden `ref` / `source` return a Relation whose `render()` wraps the FQN via `relation_marker`:

```sql
/* <ref:dim_products> */`proj`.`ds`.`dim_products`/* <ref:dim_products> */
/* <source:raw:jobs> */`proj`.`ds`.`jobs`/* <source:raw:jobs> */
```

Path metadata (`.database` / `.schema` / `.identifier`) stays the original. Ephemeral refs are unmarked. Materialization-time substitute of these markers is not implemented yet.

### Variable sets

Pass **exactly one** of:

1. **`variable_set_values`** — explicit list of dicts, optional **`variable_set_types`**
2. **`variable_set_relation`** — a relation (`ref` / `source` / Relation). dbt runs `SELECT *`, each row becomes one variable set, and parameter types come from the BigQuery schema. Do not pass `variable_set_types` with a relation.
3. **`variable_set_sql`** — a SQL string that returns one row per variable set (column names = parameter names). Types come from the result schema. Do not pass `variable_set_types` with SQL. Prefer a **simple** query (e.g. `UNNEST` of a few dates). The SQL may run more than once per dbt invocation (serial DDL + resolve); for expensive logic, put results in a table and use `variable_set_relation` instead.

```sql
-- explicit
{{ config(
    materialized="incremental_ext",
    unique_key="order_id",
    execute_ext={
        "variable_set_values": [
            {"store_id": 1, "region": "us"},
            {"store_id": 2, "region": "eu"},
        ],
        "variable_set_types": {"store_id": "INT64", "region": "STRING"},
        "worker_pool_size": 0,
    },
) }}

-- from a control table (columns = parameter names)
{{ config(
    materialized="incremental_ext",
    execute_ext={
        "variable_set_relation": ref("store_shards"),
        "worker_pool_size": 0,
    },
) }}

-- from ad-hoc SQL (e.g. always reprocess yesterday + today)
{{ config(
    materialized="incremental_ext",
    incremental_strategy="insert_overwrite",
    partition_by={"field": "dt", "data_type": "date"},
    execute_ext={
        "variable_set_sql": "SELECT dt FROM UNNEST([CURRENT_DATE() - 1, CURRENT_DATE()]) AS dt",
        "worker_pool_size": 0,
    },
) }}
```

Passing more than one of `variable_set_values` / `variable_set_relation` / `variable_set_sql` raises. Rows must not contain NULL in parameter columns (BigQuery query parameters cannot be NULL).

### Model config

```sql
-- models/orders_by_store.sql
{{ config(
    materialized="incremental_ext",
    unique_key="order_id",
    execute_ext={
        "variable_set_values": [
            {"store_id": 1, "region": "us"},
            {"store_id": 2, "region": "eu"},
        ],
        "variable_set_types": {"store_id": "INT64", "region": "STRING"},
        "worker_pool_size": 0,
    },
) }}

select *
from {{ source("raw", "orders") }}
where store_id = @store_id
  and region = @region
```

Each dict in `variable_set_values` is one script. The serial DDL (empty create, schema probe) binds only the first set so `@store_id` / `@region` are valid there too. Merges must be idempotent: a failure does not roll back sets that already succeeded.

The same `execute_ext` block works in `dbt_project.yml` or a `schema.yml` `config:` entry.

### script

`script` runs `compiled_code` as the `main` statement and returns no relation. Use it for a hand-written BigQuery script. With `execute_ext`, that script is fanned out the same way. Give each run its own `CREATE TEMP TABLE` if it stages rows before writing.

```sql
{{ config(
    materialized="script",
    execute_ext={
        "variable_set_values": [{"store_id": 1}, {"store_id": 2}],
        "variable_set_types": {"store_id": "INT64"},
    },
) }}

create temp table _batch as (
  select * from {{ source("raw", "orders") }} where store_id = @store_id
);
merge into {{ this }} as t
using _batch as s
on t.order_id = s.order_id
when matched then update set t.amount = s.amount
when not matched then insert (order_id, amount) values (s.order_id, s.amount)
```

### Direct call from a macro

```sql
{% macro refresh_stores(store_ids) %}
  {% set values = [] %}
  {% for store_id in store_ids %}
    {% do values.append({"store_id": store_id}) %}
  {% endfor %}

  {% set response, table = adapter.execute_ext(
      "delete from " ~ target.database ~ "." ~ target.schema ~ ".orders where store_id = @store_id",
      variable_set_values=values,
      variable_set_types={"store_id": "INT64"},
      worker_pool_size=-1,
  ) %}
{% endmacro %}
```

Omit `variable_set_values` (or pass `none`) and `execute_ext` falls back to a normal `execute`.

## Cloud SQL gateway (checkpoints)

Optional SQLMesh-style state backend on the BigQuery profile. Uses the [Cloud SQL Python Connector](https://github.com/GoogleCloudPlatform/cloud-sql-python-connector) with **IAM DB auth** (no password). Intended to replace the `dbt-webhook` → Cloud Function → Postgres path for delta checkpoints (`public.dbt_model_log`).

### Profile

```yaml
my_target:
  type: bigquery
  method: oauth  # or service-account / impersonate_service_account
  project: my-gcp-project
  dataset: analytics
  gateway:
    cloudsql:
      instance_connection_name: "my-gcp-project:us-central1:metadata"
      database: metadata
      ip_type: private          # private | public | psc
      schema_name: public
      # user: "dbt-runner@my-gcp-project.iam"  # optional; derived from SA / impersonation
      init_on_connect: true     # ensure schema on first BQ connection
      auto_migrate: true        # reserved; today only CREATE IF NOT EXISTS
```

IAM DB user for a service account is the SA email with `.gserviceaccount.com` stripped (`name@project.iam`). The runner needs `roles/cloudsql.client` + Cloud SQL Instance User on that instance.

### Startup

On the first BigQuery connection (when `init_on_connect: true`), the adapter connects to Cloud SQL and creates missing tables (see `gateway/schema.py`):

1. `dbt_model_log` (+ index)
2. `change_tracking_registry`
3. `change_tracking_log` (+ overlap / GIN indexes)

Force early init from `dbt_project.yml`:

```yaml
on-run-start:
  - "{{ adapter.gateway_ensure() }}"
```

### Adapter / macros

| Call | Role |
| --- | --- |
| `adapter.gateway_ensure()` | Connect + ensure tables |
| `adapter.gateway_get_checkpoint(db, schema, table)` | Latest successful row (for pre-hooks) |
| `adapter.gateway_set_checkpoint(...)` | Insert row (replaces `analytics.set_checkpoint` UDF) |
| `adapter.gateway_change_metadata_pooler(relations, …)` | Pool CHANGES → registry/log + `gateway.pooler.*` checkpoints |
| `adapter.gateway_get_affected_partitions(db, schema, table, start, end)` | Dirty partition ids (`None` = all, `[]` = none) |

Jinja wrappers: `gateway_ensure`, `gateway_get_checkpoint`, `gateway_set_checkpoint`, `gateway_pool_change_metadata`, `gateway_get_affected_partitions`.

### Change-metadata pooler

Mark models/sources to include them in the pool:

```yaml
# models/schema.yml or sources.yml
models:
  - name: orders
    config:
      enable_changetracking: true

sources:
  - name: raw
    tables:
      - name: events
        meta:
          enable_changetracking: true
```

Run once per invocation (typical) or from a script model:

```yaml
on-run-start:
  - "{{ gateway_pool_change_metadata(worker_pool_size=0) }}"
```

For each distinct FQN the pooler:

1. Reads partition type/field/grain from **BigQuery table metadata** (not dbt `partition_by`)
2. Enables change history if needed
3. On first see: logs `status=initial` with `partition_ids NULL` (entire table) and sets a `gateway.pooler.` checkpoint
4. Later: runs a single `CHANGES` aggregation → string partition ids + `COUNT(*)` / `COUNTIF` per `_CHANGE_TYPE`, writes an append-only log row, advances the pooler checkpoint in the same Postgres transaction

Partition id formats (UTC strings):

| Grain | Example |
| --- | --- |
| hour | `2026-09-20 14:00:00` |
| day / week / month | `2026-09-20` / week-start / month-start |

`partition_ids` on the log:

| Value | Meaning |
| --- | --- |
| `NULL` | Entire table / all partitions |
| `{}` | No changes in the window |
| `{id,…}` | Those partitions only |

Query dirty partitions for a time range (overlap on log `delta_*`):

```sql
{% set parts = gateway_get_affected_partitions(ref('orders'), start_ts, end_ts) %}
{# none → all table; [] → no changes; list → ids #}
```

Example commit (post-hook), after you switch off the remote UDF:

```sql
{% do adapter.gateway_set_checkpoint(
    invocation_id,
    this.database,
    this.schema,
    this.identifier,
    run_started_at | string,
    node_started_at | string,
    modules.datetime.datetime.utcnow() | string,
    delta_start_time | string,
    delta_end_time | string,
) %}
```

### worker_pool_size

| Value | Workers |
| --- | --- |
| `0` (default) | auto, currently 16 |
| `-1` | one worker per variable set |
| `> 0` | that many workers (maximum 1024) |

### Types

Scalar types only (`STRING`, `INT64`, `FLOAT64`, `BOOL`, `DATE`, `TIMESTAMP`, and the other BigQuery scalars). `ARRAY` and `STRUCT` are reserved and raise.

- Pass `variable_set_types` to force types.
- If omitted, types are inferred from the Python/Jinja values.
- `null` is rejected. BigQuery does not allow NULL query parameters, and inference cannot recover a type from null. Pass an explicit type only when the value itself is non-null.

### Failure and logs

Success means every variable set finished. Any failure raises after the pool has drained. Sets that already completed are left in place.

Progress (`starting` / `completed` / `failed` / `retried`) is debug-only. Retries use the same profile settings as a normal BigQuery job.

```bash
dbt run --select orders_by_store --debug
```

## Versioning

Two version strings, because dbt and PyPI do not accept the same syntax.

| String | Where | Example | Why |
| --- | --- | --- | --- |
| `version` | `dbt.adapters.bigquery.__version__` (what `dbt debug` parses) | `1.12.1` | dbt's semver rejects `1.12.1.post12` and aborts |
| `pypi_version` | PyPI / wheel name | `1.12.1.post12` | DataEng release N on top of upstream `1.12.1` |

`pypi_version` scheme:

```text
<upstream dbt-bigquery version>.post<N>
```

| PyPI version | Meaning |
| --- | --- |
| `1.12.1.post1` | First DataEng release on upstream `1.12.1` |
| `1.12.1.post2` | Second DataEng-only release, same upstream base |
| `1.12.1.post3` | Third DataEng-only release, same upstream base |
| `1.12.1.post4` | Fourth DataEng-only release, same upstream base |
| `1.12.1.post5` | Fifth DataEng-only release, same upstream base |
| `1.12.1.post6` | Sixth DataEng-only release, same upstream base |
| `1.12.1.post7` | Seventh DataEng-only release, same upstream base |
| `1.12.1.post8` | Eighth DataEng-only release, same upstream base |
| `1.12.1.post9` | Ninth DataEng-only release, same upstream base |
| `1.12.1.post10` | Tenth DataEng-only release, same upstream base |
| `1.12.1.post11` | Eleventh DataEng-only release, same upstream base |
| `1.12.1.post12` | Twelfth DataEng-only release, same upstream base |
| `1.13.0.post1` | Rebased onto upstream `1.13.0` |

On a rebase, set `version` to the new upstream number (`1.13.0`) and `pypi_version` to `1.13.0.post1`. Do not put `.postN` into `version`. Local versions (`1.12.1+dataeng.1`) cannot be uploaded to PyPI.

## Upstream

Apache-licensed code from dbt Labs; see `LICENSE`. Upstream project:
https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-bigquery

## Getting started

For BigQuery profile setup, see the [dbt BigQuery docs](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup).
