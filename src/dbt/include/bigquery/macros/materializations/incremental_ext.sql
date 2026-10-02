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
  see each other's temp tables, then MERGE (or DELETE+MERGE for
  insert_overwrite) into the shared target.
#}

{% macro bq_ext_empty_select(compiled_code) %}
  SELECT *
  FROM (
      {{ compiled_code }}
  )
  WHERE FALSE
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

  CREATE TABLE IF NOT EXISTS {{ relation }}
      {{ partition_by(partition_config) }}
      {{ cluster_by(raw_cluster_by) }}
  {{ bigquery_table_options(config, model, false) }}
  AS (
      {{ bq_ext_empty_select(compiled_code) }}
  )
{% endmacro %}

{% macro bq_ext_create_schema_probe(relation, compiled_code) %}
  CREATE OR REPLACE TABLE {{ relation }}
  AS (
      {{ bq_ext_empty_select(compiled_code) }}
  )
{% endmacro %}

{% macro bq_ext_run_serial(sql) %}
  {%- set ext = config.get('execute_ext', none) -%}
  {%- if ext is not none -%}
    {%- set resolved = bq_ext_resolve_variable_sets(ext) -%}
    {%- if resolved is none or resolved['values'] is none or resolved['values'] | length == 0 -%}
      {% do exceptions.raise_compiler_error(
        "execute_ext requires a non-empty variable_set_values list, "
        ~ "variable_set_relation with at least one row, or variable_set_sql "
        ~ "returning at least one row"
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

{#
  Partition-bucket equality for insert_overwrite DELETE / MERGE source filter.

  Column side uses partition_by.field; parameter side uses param_name (the sole
  execute_ext variable key), cast to the partition data_type. Names may differ
  (e.g. column __taken_at_utc, parameter @dt).
#}
{% macro bq_ext_partition_bucket_eq(partition_by, column_expr, param_name) %}
  {%- set param_expr = 'CAST(@' ~ param_name ~ ' AS ' ~ partition_by.data_type|upper ~ ')' -%}
  {%- if partition_by.data_type_should_be_truncated() -%}
    {{ partition_by.data_type }}_trunc({{ column_expr }}, {{ partition_by.granularity }})
      = {{ partition_by.data_type }}_trunc({{ param_expr }}, {{ partition_by.granularity }})
  {%- else -%}
    {{ column_expr }} = {{ param_expr }}
  {%- endif -%}
{% endmacro %}

{% macro bq_incremental_ext_script(
    target_relation,
    compiled_code,
    unique_key,
    partition_by,
    dest_columns,
    incremental_predicates,
    strategy='merge',
    partition_param_name=none
) %}
  {%- if strategy == 'insert_overwrite' -%}
    {%- set param_name = partition_param_name if partition_param_name is not none else partition_by.field -%}
    {%- set source_sql -%}
SELECT * FROM _dbt_ext_src
WHERE {{ bq_ext_partition_bucket_eq(partition_by, partition_by.field, param_name) }}
    {%- endset -%}
  {%- else -%}
    {%- set source_sql = 'SELECT * FROM _dbt_ext_src' -%}
  {%- endif -%}

  {%- set merge_sql = bq_generate_incremental_merge_build_sql(
      none,
      target_relation,
      source_sql,
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
  CREATE TEMP TABLE _dbt_ext_src AS (
      {{ compiled_code }}
  );
  {%- if strategy == 'insert_overwrite' %}
  DELETE FROM {{ target_relation }}
  WHERE {{ bq_ext_partition_bucket_eq(partition_by, partition_by.field, param_name) }};
  {%- endif %}
  {{ merge_sql }}
{% endmacro %}

{% materialization incremental_ext, adapter='bigquery', supported_languages=['sql'] -%}
  {%- set unique_key = config.get('unique_key') -%}

  {%- set strategy = config.get('incremental_strategy') or 'merge' -%}
  {%- if strategy not in ['merge', 'insert_overwrite'] -%}
    {% do exceptions.raise_compiler_error(
      "incremental_ext only supports incremental_strategy 'merge' or "
      ~ "'insert_overwrite' (got '" ~ strategy ~ "')"
    ) %}
  {%- endif -%}

  {%- set full_refresh_mode = should_full_refresh() -%}
  {%- set target_relation = this.incorporate(type='table') -%}
  {%- set existing_relation = load_relation(this) -%}
  {%- set raw_partition_by = config.get('partition_by', none) -%}
  {%- set partition_by = adapter.parse_partition_by(raw_partition_by) -%}
  {%- set on_schema_change = incremental_validate_on_schema_change(config.get('on_schema_change'), default='ignore') -%}
  {%- set incremental_predicates = config.get('predicates', default=none) or config.get('incremental_predicates', default=none) -%}
  {%- set ext = config.get('execute_ext', none) -%}
  {%- set insert_overwrite_values = none -%}
  {%- set insert_overwrite_types = none -%}
  {%- set partition_param_name = none -%}

  {%- if strategy == 'insert_overwrite' -%}
    {%- if unique_key is not none -%}
      {% do exceptions.raise_compiler_error(
        "incremental_ext insert_overwrite does not accept unique_key "
        ~ "(got '" ~ unique_key ~ "')"
      ) %}
    {%- endif -%}
    {%- if partition_by is none -%}
      {% do exceptions.raise_compiler_error(
        "incremental_ext insert_overwrite requires partition_by"
      ) %}
    {%- endif -%}
    {%- if partition_by.data_type not in ['date', 'timestamp', 'datetime'] -%}
      {% do exceptions.raise_compiler_error(
        "incremental_ext insert_overwrite requires partition_by.data_type of "
        ~ "date, timestamp, or datetime (got '" ~ partition_by.data_type
        ~ "'). Integer/range partitioning is not supported."
      ) %}
    {%- endif -%}
    {%- if partition_by.copy_partitions -%}
      {% do exceptions.raise_compiler_error(
        "incremental_ext insert_overwrite does not support copy_partitions "
        ~ "(upstream dbt partition-copy path). It always DELETE+MERGE's the "
        ~ "matching partition bucket."
      ) %}
    {%- endif -%}
    {%- if ext is none -%}
      {% do exceptions.raise_compiler_error(
        "incremental_ext insert_overwrite requires execute_ext with a variable "
        ~ "set that has exactly one parameter (name may differ from "
        ~ "partition_by.field '" ~ partition_by.field ~ "')"
      ) %}
    {%- endif -%}
    {%- set resolved = bq_ext_resolve_variable_sets(ext) -%}
    {%- if resolved is none or resolved['values'] is none or resolved['values'] | length == 0 -%}
      {% do exceptions.raise_compiler_error(
        "incremental_ext insert_overwrite requires a non-empty variable set "
        ~ "(variable_set_values, variable_set_relation, or variable_set_sql)"
      ) %}
    {%- endif -%}
    {%- set insert_overwrite_values = adapter.validate_insert_overwrite_variable_sets(
        resolved['values'],
        resolved['types'],
        partition_by.field,
        partition_by.data_type,
        partition_by.granularity
    ) -%}
    {%- set insert_overwrite_types = resolved['types'] -%}
    {%- set partition_param_name = insert_overwrite_values[0].keys() | list | first -%}
  {%- endif -%}

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

  {%- set script_sql -%}
    {{ bq_incremental_ext_script(
        target_relation,
        compiled_code,
        unique_key,
        partition_by,
        dest_columns,
        incremental_predicates,
        strategy,
        partition_param_name
    ) }}
  {%- endset -%}

  {%- if strategy == 'insert_overwrite' -%}
    {%- if execute -%}
      {{ log('Writing runtime sql for node "' ~ model['unique_id'] ~ '"') }}
      {{ write(script_sql) }}
      {%- set worker_pool_size = ext.get('worker_pool_size', 0) -%}
      {%- if worker_pool_size is none -%}
        {%- set worker_pool_size = 0 -%}
      {%- endif -%}
      {%- set res, table = adapter.execute_ext(
          script_sql,
          auto_begin=true,
          fetch=false,
          variable_set_values=insert_overwrite_values,
          variable_set_types=insert_overwrite_types,
          worker_pool_size=worker_pool_size,
      ) -%}
      {{ store_result('main', response=res, agate_table=table) }}
    {%- endif -%}
  {%- else -%}
    {%- call statement('main') -%}
      {{ script_sql }}
    {%- endcall -%}
  {%- endif -%}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}
