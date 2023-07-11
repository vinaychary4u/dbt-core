{%- macro refresh_materialized_view_template(materialized_view) -%}
    {{- adapter.dispatch('refresh_materialized_view_template', 'dbt')(materialized_view) -}}
{%- endmacro -%}


{%- macro default__refresh_materialized_view_template(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`refresh_materialized_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
