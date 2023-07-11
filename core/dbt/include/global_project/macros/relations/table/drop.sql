{%- macro drop_table_template(table) -%}
    {{- adapter.dispatch('drop_table_template', 'dbt')(table) -}}
{%- endmacro -%}

{%- macro default__drop_table_template(table) -%}
    {{- exceptions.raise_compiler_error("`drop_table_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
