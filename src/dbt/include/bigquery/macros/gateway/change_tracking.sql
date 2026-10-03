{# Change-metadata pooler: graph scan + gateway APIs. #}

{% macro _gateway_node_enable_changetracking(node) -%}
  {%- if node.config.get('enable_changetracking') -%}
    {{ return(true) }}
  {%- endif -%}
  {%- if node.meta is mapping and node.meta.get('enable_changetracking') -%}
    {{ return(true) }}
  {%- endif -%}
  {{ return(false) }}
{%- endmacro %}

{% macro _gateway_collect_changetracking_relations() -%}
  {%- if not execute -%}
    {{ return([]) }}
  {%- endif -%}

  {%- set relations = [] -%}
  {%- set seen = {} -%}

  {%- for node in graph.nodes.values() -%}
    {%- if node.resource_type in ['model', 'seed', 'snapshot']
          and _gateway_node_enable_changetracking(node)
          and node.config.get('materialized') not in ['view', 'ephemeral'] -%}
      {%- set ident = node.alias if node.alias else node.name -%}
      {%- set key = node.database ~ '.' ~ node.schema ~ '.' ~ ident -%}
      {%- if key not in seen -%}
        {%- do seen.update({key: true}) -%}
        {%- do relations.append({
            'database': node.database,
            'schema': node.schema,
            'identifier': ident,
            'node_id': node.unique_id,
        }) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}

  {%- for src in graph.sources.values() -%}
    {%- if _gateway_node_enable_changetracking(src) -%}
      {%- set key = src.database ~ '.' ~ src.schema ~ '.' ~ src.identifier -%}
      {%- if key not in seen -%}
        {%- do seen.update({key: true}) -%}
        {%- do relations.append({
            'database': src.database,
            'schema': src.schema,
            'identifier': src.identifier,
            'node_id': src.unique_id,
        }) -%}
      {%- endif -%}
    {%- endif -%}
  {%- endfor -%}

  {{ return(relations) }}
{%- endmacro %}

{% macro gateway_pool_change_metadata(worker_pool_size=0, write_bq=false, end_ts=none, bq_mirror_table=none) %}
  {%- if not execute -%}
    {{ return([]) }}
  {%- endif -%}

  {%- set relations = _gateway_collect_changetracking_relations() -%}
  {{ return(adapter.gateway_change_metadata_pooler(
      relations,
      worker_pool_size=worker_pool_size,
      write_bq=write_bq,
      end_ts=end_ts,
      invocation_id=invocation_id,
      bq_mirror_table=bq_mirror_table
  )) }}
{% endmacro %}

{% macro gateway_get_affected_partitions(relation, start_ts, end_ts) %}
  {%- if not execute -%}
    {{ return(none) }}
  {%- endif -%}
  {{ return(adapter.gateway_get_affected_partitions(
      relation.database,
      relation.schema,
      relation.identifier,
      start_ts | string,
      end_ts | string
  )) }}
{% endmacro %}

{% macro gateway_change_metadata_pooler(relations, worker_pool_size=0, write_bq=false, end_ts=none, bq_mirror_table=none) %}
  {{ return(adapter.gateway_change_metadata_pooler(
      relations,
      worker_pool_size=worker_pool_size,
      write_bq=write_bq,
      end_ts=end_ts,
      invocation_id=invocation_id,
      bq_mirror_table=bq_mirror_table
  )) }}
{% endmacro %}
