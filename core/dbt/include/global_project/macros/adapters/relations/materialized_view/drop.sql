{% macro drop_materialized_view(relation) -%}
  {{ return(adapter.dispatch('drop_materialized_view_template', 'dbt')(relation)) }}
{%- endmacro %}


{% macro default__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }} cascade
{%- endmacro %}
