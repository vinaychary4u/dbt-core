{%- macro alter_sql(existing_materialization, new_materialization) -%}

    {{- log('Applying ALTER to: ' ~ existing_materialization) -}}

    {{- adapter.dispatch('alter_sql', 'dbt')(existing_materialization, new_materialization) -}}

{%- endmacro -%}


{%- macro default__alter_sql(existing_materialization, new_materialization) -%}

    {%- set relation_type = existing_materialization.type -%}

    {%- if relation_type == 'materialized_view' -%}
        {{ alter_materialized_view_sql(existing_materialization, new_materialization) }}
    {%- else -%}
        {{- exceptions.raise_compiler_error("`alter_sql()` has not been implemented for the relation type" ~ relation_type ) -}}
    {%- endif -%}

{%- endmacro -%}
