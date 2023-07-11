{%- macro drop_view_template(view) -%}
    {{- adapter.dispatch('drop_view_template', 'dbt')(view) -}}
{%- endmacro -%}

{%- macro default__drop_view_template(view) -%}
    {{- exceptions.raise_compiler_error("`drop_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
