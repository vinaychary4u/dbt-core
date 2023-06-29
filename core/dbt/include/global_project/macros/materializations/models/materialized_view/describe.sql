{# /*
    This needs to be a {% do return(...) %} because the macro returns a dictionary, not a template.
*/ #}

{%- macro describe_materialized_view_sql(materialized_view) -%}
    {%- do return(adapter.dispatch('describe_materialized_view_sql', 'dbt')(materialized_view)) -%}
{%- endmacro -%}


{%- macro default__describe_materialized_view_sql(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`describe_materialized_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
