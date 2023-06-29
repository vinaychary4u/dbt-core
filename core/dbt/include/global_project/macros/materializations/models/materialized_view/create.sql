{%- macro create_materialized_view_sql(materialized_view) -%}
    {{- adapter.dispatch('create_materialized_view_sql', 'dbt')(materialized_view) -}}
{%- endmacro -%}


{%- macro default__create_materialized_view_sql(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`create_materialized_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
