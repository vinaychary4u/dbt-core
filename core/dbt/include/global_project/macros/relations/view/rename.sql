{%- macro rename_view_template(view, new_name) -%}
    {{- adapter.dispatch('rename_view_template', 'dbt')(view, new_name) -}}
{%- endmacro -%}

{%- macro default__rename_view_template(view, new_name) -%}
    {{- exceptions.raise_compiler_error("`rename_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
