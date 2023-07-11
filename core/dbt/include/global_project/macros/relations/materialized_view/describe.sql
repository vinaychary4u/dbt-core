{# /*
    This needs to be a {% do return(...) %} because the macro returns a dictionary, not a template.
*/ #}

{%- macro describe_materialized_view_template(materialized_view) -%}
    {%- do return(adapter.dispatch('describe_materialized_view_template', 'dbt')(materialized_view)) -%}
{%- endmacro -%}


{%- macro default__describe_materialized_view_template(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`describe_materialized_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
