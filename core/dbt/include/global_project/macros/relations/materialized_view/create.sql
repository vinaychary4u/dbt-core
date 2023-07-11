{%- macro create_materialized_view_template(materialized_view) -%}
    {{- adapter.dispatch('create_materialized_view_template', 'dbt')(materialized_view) -}}
{%- endmacro -%}


{%- macro default__create_materialized_view_template(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`create_materialized_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
