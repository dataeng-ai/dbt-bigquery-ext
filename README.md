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

Runs the model's main SQL once per variable set, in parallel, as a [BigQuery parameterized query](https://docs.cloud.google.com/bigquery/docs/parameterized-queries) (`@name` placeholders). Built-in materializations (`table`, `view`, `incremental`, and so on) pick this up automatically: the adapter overrides the core `statement` macro and, when `config.execute_ext` is set, sends only the `main` statement through `adapter.execute_ext`. Helper SQL (`run_query`, temp relations, alters) stays on the normal single `execute` path.

Without `execute_ext` config, behavior matches upstream `dbt-bigquery`.

### Model config

```sql
-- models/orders_by_store.sql
{{ config(
    materialized="table",
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

`statement('main')` submits `create or replace table … as ( <model sql> )`, so `@store_id` and `@region` are bound on that statement. Each dict in `variable_set_values` is one query. All sets must be idempotent: a failure does not roll back sets that already succeeded.

The same config block works in `dbt_project.yml` or a `schema.yml` `config:` entry.

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

Versions follow PEP 440 as:

```text
<upstream dbt-bigquery version>.post<N>
```

Examples:

| Version | Meaning |
| --- | --- |
| `1.12.1.post1` | First DataEng release on top of upstream `1.12.1` |
| `1.12.1.post2` | Second DataEng-only release, still based on `1.12.1` |
| `1.13.0.post1` | Rebased onto upstream `1.13.0`, first DataEng release |

Avoid schemes like `1.12-0.0.1` (not valid PEP 440) or local versions
(`1.12.1+dataeng.1`) — those cannot be uploaded to PyPI.

## Upstream

Apache-licensed code from dbt Labs; see `LICENSE`. Upstream project:
https://github.com/dbt-labs/dbt-adapters/tree/main/dbt-bigquery

## Getting started

For BigQuery profile setup, see the [dbt BigQuery docs](https://docs.getdbt.com/docs/core/connect-data-platform/bigquery-setup).
