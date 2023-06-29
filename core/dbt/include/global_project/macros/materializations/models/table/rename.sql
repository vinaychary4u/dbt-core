{%- macro rename_table_sql(table, new_name) -%}
    {{- adapter.dispatch('rename_table_sql', 'dbt')(table, new_name) -}}
{%- endmacro -%}


{%- macro default__rename_table_sql(table, new_name) -%}
    {{- exceptions.raise_compiler_error("`rename_table_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}
