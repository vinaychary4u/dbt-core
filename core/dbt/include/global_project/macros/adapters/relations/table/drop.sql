{% macro drop_table(relation) -%}
  {{ return(adapter.dispatch('drop_table', 'dbt')(relation)) }}
{%- endmacro %}


{% macro default__drop_table(relation) -%}
    drop table if exists {{ relation }} cascade
{%- endmacro %}
