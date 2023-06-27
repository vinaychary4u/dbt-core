{% macro create_materialized_view_sql(materialized_view, intermediate=False) -%}
    {{- log('Applying CREATE to: ' ~ materialized_view.fully_qualified_path) -}}
    {{- adapter.dispatch('create_materialized_view_sql', 'dbt')(materialized_view, intermediate) -}}
{%- endmacro %}


{% macro default__create_materialized_view_sql(materialized_view, intermediate=False) -%}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
