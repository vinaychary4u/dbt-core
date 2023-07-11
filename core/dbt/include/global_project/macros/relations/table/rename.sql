{%- macro rename_table_template(table, new_name) -%}
    {{- adapter.dispatch('rename_table_template', 'dbt')(table, new_name) -}}
{%- endmacro -%}

{%- macro default__rename_table_template(table, new_name) -%}
    {{- exceptions.raise_compiler_error("`rename_table_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
