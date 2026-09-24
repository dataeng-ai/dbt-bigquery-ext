{#
  incremental_ext

  Never CTAS into the target or a shared dataset __dbt_tmp. Parallel
  execute_ext jobs would otherwise replace the same table.

  Serial, once per run:
    1. On --full-refresh, DROP the existing relation so partition/cluster
       changes are not blocked by the old table.
    2. CREATE TABLE IF NOT EXISTS <target> AS SELECT * FROM (<model>) WHERE FALSE
    3. if on_schema_change != ignore, same empty shape into a dataset temp,
       then process_schema_changes, then drop the temp

  Main statement (this is what execute_ext fans out): one BigQuery script per
  variable set. CREATE TEMP TABLE is job-scoped, so concurrent scripts do not
  see each other's temp tables, then MERGE into the shared target.
#}

{% macro bq_ext_empty_select(compiled_code) %}
  select * from (
    {{ compiled_code }}
  )
  where false
{% endmacro %}

{% macro bq_ext_create_table_if_not_exists(relation, compiled_code) %}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set raw_cluster_by = config.get('cluster_by', none) -%}
  {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
  {%- if partition_config.time_ingestion_partitioning -%}
    {% do exceptions.raise_compiler_error(
      "incremental_ext does not support ingestion-time partitioning"
    ) %}
  {%- endif -%}

  create table if not exists {{ relation }}
    {{ partition_by(partition_config) }}
    {{ cluster_by(raw_cluster_by) }}
    {{ bigquery_table_options(config, model, false) }}
  as (
    {{ bq_ext_empty_select(compiled_code) }}
  )
{% endmacro %}

{% macro bq_ext_create_schema_probe(relation, compiled_code) %}
  create or replace table {{ relation }}
  as (
    {{ bq_ext_empty_select(compiled_code) }}
  )
{% endmacro %}

{# Run SQL once. When execute_ext is set, bind only the first variable set so
   @parameters in the model SQL are valid. This must not be statement('main'). #}
{% macro bq_ext_run_serial(sql) %}
  {%- set ext = config.get('execute_ext', none) -%}
  {%- if ext is not none -%}
    {%- set resolved = bq_ext_resolve_variable_sets(ext) -%}
    {%- if resolved is none or resolved['values'] is none or resolved['values'] | length == 0 -%}
      {% do exceptions.raise_compiler_error(
        "execute_ext requires a non-empty variable_set_values list or "
        ~ "variable_set_relation with at least one row"
      ) %}
    {%- endif -%}
    {% do adapter.execute_ext(
      sql,
      variable_set_values=[resolved['values'][0]],
      variable_set_types=resolved['types'],
      worker_pool_size=1,
    ) %}
  {%- else -%}
    {%- call statement('ext_serial') -%}
      {{ sql }}
    {%- endcall -%}
  {%- endif -%}
{% endmacro %}

{% macro bq_incremental_ext_script(target_relation, compiled_code, unique_key, partition_by, dest_columns, incremental_predicates) %}
  {%- set merge_sql = bq_generate_incremental_merge_build_sql(
      none,
      target_relation,
      'select * from _dbt_ext_src',
      unique_key,
      partition_by,
      dest_columns,
      false,
      incremental_predicates
  ) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- if sql_header is not none -%}
    {%- set merge_sql = merge_sql | replace(sql_header, '') -%}
  {%- endif -%}

  {{ sql_header if sql_header is not none }}
  create temp table _dbt_ext_src as (
    {{ compiled_code }}
  );
  {{ merge_sql }}
{% endmacro %}

{% materialization incremental_ext, adapter='bigquery', supported_languages=['sql'] -%}

  {# unique_key optional: omitted => insert-only MERGE (append), same as incremental. #}
  {%- set unique_key = config.get('unique_key') -%}

  {%- set strategy = config.get('incremental_strategy') or 'merge' -%}
  {%- if strategy != 'merge' -%}
    {% do exceptions.raise_compiler_error(
      "incremental_ext only supports incremental_strategy 'merge' (got '" ~ strategy ~ "')"
    ) %}
  {%- endif -%}

  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_predicates = config.get('predicates', default=none) or config.get('incremental_predicates', default=none) -%}
  {%- set grant_config = config.get('grants') -%}

  {{ run_hooks(pre_hooks) }}

  {%- if full_refresh_mode and existing_relation is not none -%}
    {{ adapter.drop_relation(existing_relation) }}
  {%- elif existing_relation is not none and not existing_relation.is_table -%}
    {{ adapter.drop_relation(existing_relation) }}
  {%- endif -%}

  {% set create_sql %}
    {{ bq_ext_create_table_if_not_exists(target_relation, compiled_code) }}
  {% endset %}
  {% do bq_ext_run_serial(create_sql) %}

  {%- if on_schema_change != 'ignore' -%}
    {%- set schema_probe = make_temp_relation(this) -%}
    {% set probe_sql %}
      {{ bq_ext_create_schema_probe(schema_probe, compiled_code) }}
    {% endset %}
    {% do bq_ext_run_serial(probe_sql) %}
    {% set _ = adapter.dispatch('process_schema_changes', 'dbt')(on_schema_change, schema_probe, target_relation) %}
    {{ adapter.drop_relation(schema_probe) }}
  {%- endif -%}

  {%- set dest_columns = adapter.get_columns_in_relation(target_relation) -%}

  {%- call statement('main') -%}
    {{ bq_incremental_ext_script(
        target_relation,
        compiled_code,
        unique_key,
        partition_by,
        dest_columns,
        incremental_predicates
    ) }}
  {%- endcall -%}

  {{ run_hooks(post_hooks) }}

  {% set should_revoke = should_revoke(existing_relation, full_refresh_mode) %}
  {% do apply_grants(target_relation, grant_config, should_revoke) %}
  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
