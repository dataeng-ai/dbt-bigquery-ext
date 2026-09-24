{# Resolve execute_ext config to {values, types}, or none when neither source is set. #}
{% macro bq_ext_resolve_variable_sets(ext) %}
  {%- if ext is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- set values = ext.get('variable_set_values') -%}
  {%- set types = ext.get('variable_set_types') -%}
  {%- set relation = ext.get('variable_set_relation') -%}
  {{ return(adapter.resolve_execute_ext_variable_sets(
      variable_set_values=values,
      variable_set_types=types,
      variable_set_relation=relation,
  )) }}
{% endmacro %}
