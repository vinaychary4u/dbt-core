{%- macro rename_view_sql(view, new_name) -%}
    {{- adapter.dispatch('rename_view_sql', 'dbt')(view, new_name) -}}
{%- endmacro -%}


{%- macro default__rename_view_sql(view, new_name) -%}
    {{- exceptions.raise_compiler_error("`rename_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
