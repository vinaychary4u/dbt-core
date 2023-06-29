{%- macro rename_materialized_view_sql(materialized_view, new_name) -%}
    {{- adapter.dispatch('rename_materialized_view_sql', 'dbt')(materialized_view, new_name) -}}
{%- endmacro -%}


{%- macro default__rename_materialized_view_sql(materialized_view, new_name) -%}
    {{- exceptions.raise_compiler_error("`rename_materialized_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
