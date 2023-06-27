{% macro alter_materialized_view_sql(new_materialized_view, existing_materialized_view) %}
    {{- log('Applying ALTER to: ' ~ new_materialized_view.fully_qualified_path) -}}
    {{- adapter.dispatch('alter_materialized_view_sql', 'dbt')(new_materialized_view, existing_materialized_view) -}}
{% endmacro %}


{% macro default__alter_materialized_view_sql(new_materialized_view, existing_materialized_view) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
