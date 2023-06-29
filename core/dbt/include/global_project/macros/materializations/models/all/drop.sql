{%- macro drop_sql(materialization) -%}

    {{- log('Applying DROP to: ' ~ materialization) -}}

    {%- set relation = adapter.base_relation_from_materialization_config(materialization) -%}

    {{- adapter.dispatch('drop_sql', 'dbt')(materialization) -}}

    {{- adapter.cache_dropped(relation) -}}

{%- endmacro -%}


{%- macro default__drop_sql(materialization) -%}

    {%- set relation_type = materialization.type -%}

    {%- if relation_type == 'materialized_view' -%}
        {{ drop_materialized_view_sql(materialization) }}
    {%- else -%}
        {{- exceptions.raise_compiler_error("`drop_sql()` has not been implemented for the relation type" ~ relation_type ) -}}
    {%- endif -%}

{%- endmacro -%}
