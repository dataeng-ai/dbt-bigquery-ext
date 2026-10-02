{#
  Wrap builtins.ref so Relation.render() emits deterministic relation markers:
    /* <ref:…> */`proj`.`ds`.`tbl`/* <ref:…> */

  Active when mark_relations=true, or materialized in (incremental_ext, script).
  Ephemeral CTEs are unmarked.
#}
{% macro ref() %}
  {%- set rel = builtins.ref(*varargs, **kwargs) -%}
  {%- if not adapter.should_mark_relations(config) or rel.is_cte -%}
    {{ return(rel) }}
  {%- endif -%}

  {%- set parts = [] -%}
  {%- for arg in varargs -%}
    {%- do parts.append(arg) -%}
  {%- endfor -%}
  {%- if kwargs.get("version") is not none -%}
    {%- do parts.append("v" ~ kwargs.get("version")) -%}
  {%- endif -%}

  {%- set marker = adapter.make_relation_marker_id("ref", parts) -%}
  {{ return(adapter.mark_relation(rel, marker)) }}
{% endmacro %}
