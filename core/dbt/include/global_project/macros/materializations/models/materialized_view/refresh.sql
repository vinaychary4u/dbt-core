{% macro refresh_materialized_view(materialized_view) %}
    {{- log('Applying REFRESH to: ' ~ materialized_view.fully_qualified_path) -}}
    {{- adapter.dispatch('refresh_materialized_view', 'dbt')(materialized_view) -}}
{% endmacro %}


{% macro default__refresh_materialized_view(materialized_view) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
