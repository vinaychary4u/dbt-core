{% macro replace_materialized_view_sql(new_materialized_view, existing_relation) %}
    {{- log('Applying REPLACE to: ' ~ new_materialized_view.fully_qualified_path) -}}
    {{- adapter.dispatch('replace_materialized_view_sql', 'dbt')(new_materialized_view, existing_relation) -}}
{% endmacro %}


{% macro default__replace_materialized_view_sql(new_materialized_view, existing_relation) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
