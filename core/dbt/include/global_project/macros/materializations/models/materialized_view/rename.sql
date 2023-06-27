{% macro rename_materialized_view(materialized_view, name, intermediate=False) %}
    {{- log('Applying RENAME to: ' ~ materialized_view.fully_qualified_path) -}}
    {{- adapter.dispatch('rename_materialized_view', 'dbt')(materialized_view, name, intermediate) -}}
{% endmacro %}


{% macro default__rename_materialized_view(materialized_view, name, intermediate=False) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
