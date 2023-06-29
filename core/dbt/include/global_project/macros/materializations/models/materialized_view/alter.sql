{%- macro alter_materialized_view_sql(existing_materialized_view, new_materialized_view) -%}
    {{- adapter.dispatch('alter_materialized_view_sql', 'dbt')(existing_materialized_view, new_materialized_view) -}}
{%- endmacro -%}


{%- macro default__alter_materialized_view_sql(existing_materialized_view, new_materialized_view) -%}
    {{- exceptions.raise_compiler_error("`alter_materialized_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
