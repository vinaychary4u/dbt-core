{# /*
    This macro is elevated to `all` because it only applies to materialized views.
*/ #}

{%- macro refresh_materialized_view_sql(materialized_view) -%}

    {{- log('Applying REFRESH to: ' ~ materialized_view) -}}

    {{- adapter.dispatch('refresh_materialized_view_sql', 'dbt')(materialized_view) -}}

{%- endmacro -%}


{%- macro default__refresh_materialized_view_sql(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`refresh_materialized_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
