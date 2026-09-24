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

Merge-only incremental materialization. It does not `CREATE OR REPLACE` the target and it does not build a shared dataset `__dbt_tmp`.

Serial, once per run:

1. On `--full-refresh`, `DROP` the existing relation. Truncate would keep the old partition and cluster spec and fail when those change.
2. `CREATE TABLE IF NOT EXISTS <target> AS SELECT * FROM (<model>) WHERE FALSE` so a missing table is created empty, with the current partition and cluster config.
3. When `on_schema_change` is not `ignore`, create a dataset temp the same empty way, apply the schema change to the target, then drop the temp.

The `main` statement is one BigQuery script per variable set:

```sql
create temp table _dbt_ext_src as (
  <model sql, with @parameters>
);
merge into <target> ... using (select * from _dbt_ext_src) ...
```

`CREATE TEMP TABLE` lives in that script's job, so concurrent `execute_ext` workers do not see each other's temp tables. Each merge writes only its own rows into the shared target. `unique_key` is optional: with it, matched rows update; without it, the merge is insert-only (append), same as regular `incremental` + `merge`. `incremental_strategy` must be `merge` (the default).

### Variable sets

Pass **exactly one** of:

1. **`variable_set_values`** — explicit list of dicts, optional **`variable_set_types`**
2. **`variable_set_relation`** — a relation (`ref` / `source` / Relation). dbt runs `SELECT *`, each row becomes one variable set, and parameter types come from the BigQuery schema. Do not pass `variable_set_types` with a relation.

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
```

Passing both `variable_set_values` and `variable_set_relation` raises. Rows must not contain NULL in parameter columns (BigQuery query parameters cannot be NULL).

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
  project: simbe-data-dev
  dataset: analytics
  gateway:
    cloudsql:
      instance_connection_name: "simbe-data-prd:us-central1:metadata"
      database: metadata
      ip_type: private          # private | public | psc
      schema_name: public
      # user: "dbt-runner@simbe-data-prd.iam"  # optional; derived from SA / impersonation
      init_on_connect: true     # ensure schema on first BQ connection
      auto_migrate: true        # reserved; today only CREATE IF NOT EXISTS
```

IAM DB user for a service account is the SA email with `.gserviceaccount.com` stripped (`name@project.iam`). The runner needs `roles/cloudsql.client` + Cloud SQL Instance User on that instance.

### Startup

On the first BigQuery connection (when `init_on_connect: true`), the adapter connects to Cloud SQL and:

1. Checks for `dbt_model_log`
2. If missing → `CREATE TABLE` + index (same DDL as Simbe `data-tf` `pg_schema/dbt_model_log.sql`)
3. If present → skip

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

Jinja wrappers: `gateway_ensure`, `gateway_get_checkpoint`, `gateway_set_checkpoint`.

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
| `version` | `dbt.adapters.bigquery.__version__` (what `dbt debug` parses) | `1.12.1` | dbt's semver rejects `1.12.1.post6` and aborts |
| `pypi_version` | PyPI / wheel name | `1.12.1.post6` | DataEng release N on top of upstream `1.12.1` |

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
| `1.13.0.post1` | Rebased onto upstream `1.13.0` |

On a rebase, set `version` to the new upstream number (`1.13.0`) and `pypi_version` to `1.13.0.post1`. Do not put `.postN` into `version`. Local versions (`1.12.1+dataeng.1`) cannot be uploaded to PyPI.

## Upstream

Apache-licensed code from dbt Labs; see `LICENSE`. Upstream project:
https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-bigquery

## Getting started

For BigQuery profile setup, see the [dbt BigQuery docs](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup).
