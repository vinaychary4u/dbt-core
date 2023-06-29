{%- macro create_sql(materialization) -%}

    {{- log('Applying CREATE to: ' ~ materialization) -}}

    {{- adapter.dispatch('create_sql', 'dbt')(materialization) -}}

{%- endmacro -%}


{%- macro default__create_sql(materialization) -%}

    {%- set relation_type = materialization.type -%}

    {%- if relation_type == 'materialized_view' -%}
        {{ create_materialized_view_sql(materialization) }}
    {%- else -%}
        {{- exceptions.raise_compiler_error("`create_sql()` has not been implemented for the relation type" ~ relation_type ) -}}
    {%- endif -%}

{%- endmacro -%}
