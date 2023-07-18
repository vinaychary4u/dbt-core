{% macro drop_view(relation) -%}
  {{ return(adapter.dispatch('drop_view', 'dbt')(relation)) }}
{%- endmacro %}


{% macro default__drop_view(relation) -%}
    drop view if exists {{ relation }} cascade
{%- endmacro %}
