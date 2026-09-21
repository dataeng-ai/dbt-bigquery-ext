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

## Extensions

- `adapter.execute_ext(...)` — parameterized parallel BigQuery queries (`variable_set_values`,
  `worker_pool_size`, `variable_set_types`). See connection manager docs in source.

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
