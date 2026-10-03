{#
    Override only the unique-key match used inside the MERGE ON predicate.

    When the `enable_truthy_nulls_equals_macro` flag is enabled, `bigquery__equals`
    emits `IS NOT DISTINCT FROM`. Inside a MERGE on a partitioned table that has
    `require_partition_filter=True`, BigQuery's partition-pruning analyzer no longer
    recognizes the `(<partition_field> is null or <partition_field> is not null)`
    auxiliary predicate (added by `predicate_for_avoid_require_partition_filter`)
    as a valid partition filter, and the MERGE fails at runtime. Use the
    equivalent `(a is null and b is null) or (a = b)` form instead so partition
    pruning still works.

    Note: IS DISTINCT FROM on the WHEN MATCHED change-check (merge_skip_unchanged)
    is fine — that predicate is not used for partition pruning.
#}
{% macro bigquery__get_merge_unique_key_match(source_unique_key, target_unique_key) -%}
    {%- if adapter.behavior.enable_truthy_nulls_equals_macro.no_warn -%}
        (({{ source_unique_key }} IS NULL AND {{ target_unique_key }} IS NULL) OR ({{ source_unique_key }} = {{ target_unique_key }}))
    {%- else -%}
        ({{ source_unique_key }} = {{ target_unique_key }})
    {%- endif %}
{%- endmacro %}


{#
  merge_skip_unchanged (Option B)

  When merge_skip_unchanged=true and unique_key is set, emit:
    WHEN MATCHED AND (<stable STRUCT> IS DISTINCT FROM <dest STRUCT>) THEN UPDATE ...

  Column scope (XOR, at most one):
    - stable_columns: compare only these
    - unstable_columns: compare all dest columns except these
    - neither: compare all dest columns
  unique_key columns are always omitted from the compare set.

  Prefer STRUCT ... IS DISTINCT FROM over TO_JSON_STRING/FARM_FINGERPRINT:
  null-safe, no JSON serialization cost, groupable field types only
  (GEOGRAPHY/JSON/etc. are not supported in the compare STRUCT).
#}

{% macro bq_normalize_unique_key_list(unique_key) %}
  {%- if unique_key is none -%}
    {{ return([]) }}
  {%- elif unique_key is string -%}
    {{ return([unique_key]) }}
  {%- else -%}
    {{ return(unique_key | list) }}
  {%- endif -%}
{% endmacro %}


{% macro bq_resolve_merge_compare_columns(dest_columns, unique_key) %}
  {%- set skip = config.get('merge_skip_unchanged') -%}
  {%- if not skip -%}
    {{ return(none) }}
  {%- endif -%}

  {%- if not unique_key -%}
    {% do exceptions.raise_compiler_error(
      "merge_skip_unchanged requires unique_key (merge WHEN MATCHED updates only)"
    ) %}
  {%- endif -%}

  {%- set stable = config.get('stable_columns') -%}
  {%- set unstable = config.get('unstable_columns') -%}
  {%- if stable is not none and unstable is not none -%}
    {% do exceptions.raise_compiler_error(
      "merge_skip_unchanged: pass only one of stable_columns or unstable_columns, not both"
    ) %}
  {%- endif -%}

  {%- set uk_lower = bq_normalize_unique_key_list(unique_key) | map('lower') | list -%}
  {%- set dest_names = dest_columns | map(attribute='column') | list -%}
  {%- set dest_lower = dest_names | map('lower') | list -%}

  {%- set compare = [] -%}
  {%- if stable is not none -%}
    {%- for col in stable -%}
      {%- if col | lower in uk_lower -%}
        {# unique_key cols are redundant in the change check; skip silently #}
      {%- elif col | lower not in dest_lower -%}
        {% do exceptions.raise_compiler_error(
          "merge_skip_unchanged stable_columns entry '" ~ col ~ "' is not in the target relation"
        ) %}
      {%- else -%}
        {%- do compare.append(col) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- else -%}
    {%- set unstable_lower = (unstable | default([])) | map('lower') | list -%}
    {%- for col in dest_names -%}
      {%- if col | lower not in uk_lower and col | lower not in unstable_lower -%}
        {%- do compare.append(col) -%}
      {%- endif -%}
    {%- endfor -%}
  {%- endif -%}

  {%- if compare | length == 0 -%}
    {% do exceptions.raise_compiler_error(
      "merge_skip_unchanged resolved an empty compare column set; "
      ~ "check stable_columns / unstable_columns"
    ) %}
  {%- endif -%}

  {{ return(compare) }}
{% endmacro %}


{% macro bq_merge_skip_unchanged_predicate(compare_columns, source_alias='DBT_INTERNAL_SOURCE', dest_alias='DBT_INTERNAL_DEST') %}
  STRUCT(
    {%- for col in compare_columns %}
    {{ source_alias }}.{{ adapter.quote(col) }}{% if not loop.last %},{% endif %}
    {%- endfor %}
  ) IS DISTINCT FROM STRUCT(
    {%- for col in compare_columns %}
    {{ dest_alias }}.{{ adapter.quote(col) }}{% if not loop.last %},{% endif %}
    {%- endfor %}
  )
{% endmacro %}


{% macro bigquery__get_merge_sql(target, source, unique_key, dest_columns, incremental_predicates=none) -%}
  {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}
  {%- set dest_cols_csv = get_quoted_csv(dest_columns | map(attribute="name")) -%}
  {%- set merge_update_columns = config.get('merge_update_columns') -%}
  {%- set merge_exclude_columns = config.get('merge_exclude_columns') -%}
  {%- set update_columns = get_merge_update_columns(merge_update_columns, merge_exclude_columns, dest_columns) -%}
  {%- set sql_header = config.get('sql_header', none) -%}
  {%- set compare_columns = bq_resolve_merge_compare_columns(dest_columns, unique_key) -%}

  {% if unique_key %}
    {% if unique_key is sequence and unique_key is not mapping and unique_key is not string %}
      {% for key in unique_key %}
        {% set this_key_match %}
          DBT_INTERNAL_SOURCE.{{ key }} = DBT_INTERNAL_DEST.{{ key }}
        {% endset %}
        {% do predicates.append(this_key_match) %}
      {% endfor %}
    {% else %}
      {% set source_unique_key = ("DBT_INTERNAL_SOURCE." ~ unique_key) | trim %}
      {% set target_unique_key = ("DBT_INTERNAL_DEST." ~ unique_key) | trim %}
      {% set unique_key_match = get_merge_unique_key_match(source_unique_key, target_unique_key) %}
      {% do predicates.append(unique_key_match) %}
    {% endif %}
  {% else %}
    {% do predicates.append('FALSE') %}
  {% endif %}

  {{ sql_header if sql_header is not none }}

  MERGE INTO {{ target }} AS DBT_INTERNAL_DEST
  USING {{ source }} AS DBT_INTERNAL_SOURCE
  ON {{"(" ~ predicates | join(") AND (") ~ ")"}}

  {% if unique_key %}
  WHEN MATCHED
  {%- if compare_columns is not none %}
    AND ({{ bq_merge_skip_unchanged_predicate(compare_columns) }})
  {%- endif %}
  THEN UPDATE SET
    {% for column_name in update_columns -%}
      {{ column_name }} = DBT_INTERNAL_SOURCE.{{ column_name }}
      {%- if not loop.last %}, {% endif %}
    {%- endfor %}
  {% endif %}

  WHEN NOT MATCHED THEN INSERT
    ({{ dest_cols_csv }})
  VALUES
    ({{ dest_cols_csv }})

{% endmacro %}


{% macro bq_generate_incremental_merge_build_sql(
    tmp_relation, target_relation, sql, unique_key, partition_by, dest_columns, tmp_relation_exists, incremental_predicates
) %}
    {%- set source_sql -%}
        {%- if tmp_relation_exists -%}
        (
        SELECT
        {% if partition_by.time_ingestion_partitioning -%}
        {{ partition_by.insertable_time_partitioning_field() }},
        {%- endif -%}
        * FROM {{ tmp_relation }}
        )
        {%- else -%} {#-- wrap sql in parens to make it a subquery --#}
        (
            {%- if partition_by.time_ingestion_partitioning -%}
            {{ wrap_with_time_ingestion_partitioning_sql(partition_by, sql, True) }}
            {%- else -%}
            {{sql}}
            {%- endif %}
        )
        {%- endif -%}
    {%- endset -%}

    {%- set predicates = [] if incremental_predicates is none else [] + incremental_predicates -%}

    {# Time-partitioned merge: prune dest via script variable populated from staging. #}
    {%- set use_partition_var = tmp_relation_exists and bq_merge_supports_partition_predicate(partition_by) -%}
    {%- if use_partition_var -%}
        {%- do predicates.append(bq_merge_partition_predicate_from_var(partition_by)) -%}
    {%- endif -%}

    {%- set avoid_require_partition_filter = predicate_for_avoid_require_partition_filter() -%}
    {%- if avoid_require_partition_filter is not none -%}
        {% do predicates.append(avoid_require_partition_filter) %}
    {%- endif -%}

    {% set merge_sql = get_merge_sql(target_relation, source_sql, unique_key, dest_columns, predicates) %}

    {%- if use_partition_var -%}
      {% set build_sql %}
{{ bq_wrap_merge_with_partition_dates(partition_by, tmp_relation, merge_sql) }}
      {% endset %}
    {%- else -%}
      {% set build_sql = merge_sql %}
    {%- endif -%}

    {{ return(build_sql) }}

{% endmacro %}
