{% macro information_schema_name(database) %}
  {{ return(adapter.dispatch('information_schema_name', 'dbt')(database)) }}
{% endmacro %}


{% macro default__information_schema_name(database) -%}
  {%- if database -%}
    {{ database }}.INFORMATION_SCHEMA
  {%- else -%}
    INFORMATION_SCHEMA
  {%- endif -%}
{%- endmacro %}
