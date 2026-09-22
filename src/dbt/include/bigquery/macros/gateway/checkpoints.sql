{# Thin wrappers so projects can switch hooks from BQ UDF / webhook to adapter. #}

{% macro gateway_ensure() %}
  {{ return(adapter.gateway_ensure()) }}
{% endmacro %}

{% macro gateway_get_checkpoint(target_database, target_schema, target_table_name) %}
  {{ return(adapter.gateway_get_checkpoint(target_database, target_schema, target_table_name)) }}
{% endmacro %}

{% macro gateway_set_checkpoint(
    invocation_id,
    target_database,
    target_schema,
    target_table_name,
    run_started_at,
    node_started_at,
    node_finished_at,
    delta_start_time,
    delta_end_time,
    success=true,
    full_refresh=none
) %}
  {{ return(adapter.gateway_set_checkpoint(
      invocation_id,
      target_database,
      target_schema,
      target_table_name,
      run_started_at,
      node_started_at,
      node_finished_at,
      delta_start_time,
      delta_end_time,
      success,
      full_refresh
  )) }}
{% endmacro %}
