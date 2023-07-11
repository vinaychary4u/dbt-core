{%- macro drop_materialized_view_template(materialized_view) -%}
    {{- adapter.dispatch('drop_materialized_view_template', 'dbt')(materialized_view) -}}
{%- endmacro -%}

{%- macro default__drop_materialized_view_template(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`drop_materialized_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
