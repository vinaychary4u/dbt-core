{%- macro alter_materialized_view_template(existing_materialized_view, target_materialized_view) -%}
    {{- adapter.dispatch('alter_materialized_view_template', 'dbt')(existing_materialized_view, target_materialized_view) -}}
{%- endmacro -%}


{%- macro default__alter_materialized_view_template(existing_materialized_view, target_materialized_view) -%}
    {{- exceptions.raise_compiler_error("`alter_materialized_view_template` has not been implemented for this adapter.") -}}
{%- endmacro -%}
