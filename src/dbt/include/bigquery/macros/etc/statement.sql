{#--
Override of dbt-core `statement`. Called directly (not via adapter.dispatch),
so this definition in the BigQuery adapter package replaces the global one.

When a model sets config.execute_ext, the primary materialization statement
(`name == 'main'`) is submitted through adapter.execute_ext. Other statements
(run_query, temp relations, alters) stay on adapter.execute so helper SQL is
not fanned out across the variable set.
--#}
{%- macro statement(name=None, fetch_result=False, auto_begin=True, language='sql') -%}
  {%- if execute: -%}
    {%- set compiled_code = caller() -%}

    {%- if name == 'main' -%}
      {{ log('Writing runtime {} for node "{}"'.format(language, model['unique_id'])) }}
      {{ write(compiled_code) }}
    {%- endif -%}

    {%- if language == 'sql' -%}
      {%- set ext = none -%}
      {%- if name == 'main' -%}
        {%- set ext = config.get('execute_ext', none) -%}
      {%- endif -%}

      {%- if ext is not none -%}
        {%- if ext is not mapping -%}
          {% do exceptions.raise_compiler_error(
            "config execute_ext must be a mapping with variable_set_values, "
            ~ "optional variable_set_types, and optional worker_pool_size"
          ) %}
        {%- endif -%}
        {%- set allowed_materializations = ['incremental_ext', 'script'] -%}
        {%- set materialized = config.get('materialized') -%}
        {%- if materialized not in allowed_materializations -%}
          {% do exceptions.raise_compiler_error(
            "config execute_ext is only supported for materializations "
            ~ allowed_materializations | join(', ')
            ~ " (got '" ~ materialized ~ "')"
          ) %}
        {%- endif -%}
        {%- set worker_pool_size = ext.get('worker_pool_size', 0) -%}
        {%- if worker_pool_size is none -%}
          {%- set worker_pool_size = 0 -%}
        {%- endif -%}
        {%- set res, table = adapter.execute_ext(
            compiled_code,
            auto_begin=auto_begin,
            fetch=fetch_result,
            variable_set_values=ext.get('variable_set_values'),
            variable_set_types=ext.get('variable_set_types'),
            worker_pool_size=worker_pool_size,
        ) -%}
      {%- else -%}
        {%- set res, table = adapter.execute(compiled_code, auto_begin=auto_begin, fetch=fetch_result) -%}
      {%- endif -%}
    {%- elif language == 'python' -%}
      {%- set res = submit_python_job(model, compiled_code) -%}
      {#-- TODO: What should table be for python models? --#}
      {%- set table = None -%}
    {%- else -%}
      {% do exceptions.raise_compiler_error("statement macro didn't get supported language") %}
    {%- endif -%}

    {%- if name is not none -%}
      {{ store_result(name, response=res, agate_table=table) }}
    {%- endif -%}

  {%- endif -%}
{%- endmacro %}
