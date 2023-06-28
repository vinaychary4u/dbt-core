{# /*
    This needs to be a {% do return(...) %} because the macro returns a dictionary, not a string.
*/ #}

{% macro describe_materialized_view_sql(materialized_view) %}
    {{- log('Applying DESCRIBE to: ' ~ materialized_view.fully_qualified_path) -}}
    {% do return(adapter.dispatch('describe_materialized_view_sql', 'dbt')(materialized_view)) %}
{% endmacro %}


{% macro default__describe_materialized_view_sql(materialized_view) %}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{% endmacro %}
