{% materialization script, adapter='bigquery', supported_languages=['sql'] -%}
  {%- call statement('main') -%}
        {{ compiled_code }}
    {% endcall %}
  {{ return({'relations': []}) }}
{%- endmaterialization %}
