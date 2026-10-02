{#
  Wrap builtins.source so Relation.render() emits deterministic relation markers:
    /* <source:…> */`proj`.`ds`.`tbl`/* <source:…> */

  Active when mark_relations=true, or materialized in (incremental_ext, script).
#}
{% macro source(source_name, table_name) %}
  {%- set rel = builtins.source(source_name, table_name) -%}
  {%- if not adapter.should_mark_relations(config) -%}
    {{ return(rel) }}
  {%- endif -%}

  {%- set marker = adapter.make_relation_marker_id("source", [source_name, table_name]) -%}
  {{ return(adapter.mark_relation(rel, marker)) }}
{% endmacro %}
