{% macro get_materialization_macro(materialization_type) -%}
      -- Reuse a fundamental materialization type (like table, view, or materialized_view)
      -- TODO: support actual dispatch for materialization macros
      -- See related tracking ticket: https://github.com/dbt-labs/dbt-core/issues/7799
      {% set base_search_name = "materialization_" ~ materialization_type ~ "_" %}
      {% set search_name = base_search_name ~ adapter.type() %}

      {% if not search_name in context %}
          {% set search_name = base_search_name ~ "default" %}
      {% endif %}
      {% set materialization_macro = context[search_name] %}
      {% do return(materialization_macro) %}
{%- endmacro %}
