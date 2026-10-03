{% macro declare_dbt_max_partition(relation, partition_by, compiled_code, language='sql') %}

  {#-- TODO: revisit partitioning with python models --#}
  {%- if '_dbt_max_partition' in compiled_code and language == 'sql' -%}

    declare _dbt_max_partition {{ partition_by.data_type_for_partition() }} default (
      select max({{ partition_by.field }}) from {{ this }}
      where {{ partition_by.field }} is not null
    );

  {%- endif -%}

{% endmacro %}

{% macro predicate_for_avoid_require_partition_filter(target='DBT_INTERNAL_DEST') %}

    {%- set raw_partition_by = config.get('partition_by', none) -%}
    {%- set partition_config = adapter.parse_partition_by(raw_partition_by) -%}
    {%- set predicate = none -%}

    {% if partition_config and config.get('require_partition_filter') -%}
        {%- set partition_field = partition_config.time_partitioning_field() if partition_config.time_ingestion_partitioning else partition_config.field -%}
        {% set predicate %}
            (
                `{{ target }}`.`{{ partition_field }}` is null
                or `{{ target }}`.`{{ partition_field }}` is not null
            )
        {% endset %}
    {%- endif -%}

    {{ return(predicate) }}

{% endmacro %}


{#
  Partition-prune helpers for incremental MERGE.

  Always applied for time-partitioned targets (date/timestamp/datetime) on
  merge — no config flag. Prefer compiling a literal DATE IN (...) list from
  the staging relation (known to prune). Script variables / UNNEST are used
  only when a staging relation is not queryable from Jinja (incremental_ext
  CREATE TEMP in the same script); those must be verified for pruning.
#}

{% macro bq_merge_partition_field(partition_by) %}
  {%- if partition_by is none -%}
    {{ return(none) }}
  {%- endif -%}
  {%- if partition_by.time_ingestion_partitioning -%}
    {{ return(partition_by.time_partitioning_field()) }}
  {%- else -%}
    {{ return(partition_by.field) }}
  {%- endif -%}
{% endmacro %}


{% macro bq_merge_supports_partition_predicate(partition_by) %}
  {%- if partition_by is none or not partition_by.field -%}
    {{ return(false) }}
  {%- endif -%}
  {%- set dtype = (partition_by.data_type or '') | lower -%}
  {{ return(dtype in ['date', 'timestamp', 'datetime']) }}
{% endmacro %}


{% macro bq_merge_dest_partition_date_expr(partition_by, target_alias='DBT_INTERNAL_DEST') %}
  {%- set field = bq_merge_partition_field(partition_by) -%}
  DATE({{ target_alias }}.{{ field }})
{% endmacro %}


{% macro bq_merge_source_partition_date_expr(partition_by, relation_or_alias=none) %}
  {%- set field = bq_merge_partition_field(partition_by) -%}
  {%- if relation_or_alias is none -%}
    DATE({{ field }})
  {%- else -%}
    {# Alias (e.g. _dbt_ext_src) only — do not pass a Relation (FQN.field is invalid). #}
    DATE({{ relation_or_alias }}.{{ field }})
  {%- endif -%}
{% endmacro %}


{% macro bq_dates_to_sql_literals(dates) %}
  {# Render as DATE 'YYYY-MM-DD', ... for a MERGE partition filter. #}
  {%- if dates is none or dates | length == 0 -%}
    {{ return("DATE '1900-01-02'") }}
  {%- endif -%}
  {%- set strs = [] -%}
  {%- for d in dates -%}
    {%- if d is not none -%}
      {%- do strs.append("DATE '" ~ (d | string)[:10] ~ "'") -%}
    {%- endif -%}
  {%- endfor -%}
  {%- if strs | length == 0 -%}
    {{ return("DATE '1900-01-02'") }}
  {%- endif -%}
  {{ return(strs | join(", ")) }}
{% endmacro %}


{% macro bq_get_relation_partition_dates(relation, partition_by) %}
  {%- if not execute -%}
    {{ return([]) }}
  {%- endif -%}
  {%- set field = bq_merge_partition_field(partition_by) -%}
  {%- set dates_sql -%}
    SELECT DISTINCT DATE({{ field }}) AS _dt
    FROM {{ relation }}
    WHERE {{ field }} IS NOT NULL
  {%- endset -%}
  {%- set result = run_query(dates_sql) -%}
  {%- if result is none -%}
    {{ return([]) }}
  {%- endif -%}
  {{ return(result.columns[0].values()) }}
{% endmacro %}


{% macro bq_merge_partition_predicate_from_dates(partition_by, dates, target_alias='DBT_INTERNAL_DEST') %}
  {{ bq_merge_dest_partition_date_expr(partition_by, target_alias) }} IN ({{ bq_dates_to_sql_literals(dates) }})
{% endmacro %}


{% macro bq_merge_partition_predicate_from_var(partition_by, var_name='_dbt_merge_partition_dates', target_alias='DBT_INTERNAL_DEST') %}
  {{ bq_merge_dest_partition_date_expr(partition_by, target_alias) }} IN UNNEST(IFNULL({{ var_name }}, [DATE '1900-01-02']))
{% endmacro %}


{% macro bq_set_merge_partition_dates_from_relation(partition_by, relation, var_name='_dbt_merge_partition_dates') %}
  SET {{ var_name }} = (
    SELECT ARRAY_AGG(dt) FROM (
      SELECT DISTINCT {{ bq_merge_source_partition_date_expr(partition_by) }} AS dt
      FROM {{ relation }}
      WHERE {{ bq_merge_partition_field(partition_by) }} IS NOT NULL
    )
  );
{% endmacro %}


{% macro bq_wrap_merge_with_partition_dates(partition_by, relation, merge_sql, var_name='_dbt_merge_partition_dates') %}
  {# Regular incremental: staging tmp already exists; DECLARE first in the script. #}
BEGIN
  DECLARE {{ var_name }} ARRAY<DATE>;

  {{ bq_set_merge_partition_dates_from_relation(partition_by, relation, var_name) }}

  {{ merge_sql }};
END;
{% endmacro %}


{% macro bq_declare_merge_partition_dates_from_temp(partition_by, temp_relation='_dbt_ext_src', var_name='_dbt_merge_partition_dates') %}
  {{ bq_set_merge_partition_dates_from_relation(partition_by, temp_relation, var_name) }}
{% endmacro %}
