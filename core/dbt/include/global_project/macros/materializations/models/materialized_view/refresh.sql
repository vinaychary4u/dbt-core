{% macro refresh_materialized_view_sql(materialized_view) %}
    {{- log('Applying REFRESH to: ' ~ materialized_view.fully_qualified_path) -}}
    {{- adapter.dispatch('refresh_materialized_view_sql', 'dbt')(materialized_view) -}}
{% endmacro %}


{% macro default__refresh_materialized_view_sql(materialized_view) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
