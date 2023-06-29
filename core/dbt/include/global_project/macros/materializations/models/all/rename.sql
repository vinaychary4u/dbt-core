{%- macro rename_sql(materialization, new_name) -%}

    {{- log('Applying RENAME to: ' ~ materialization) -}}

    {%- if adapter.is_materialization_config(materialization) -%}
        {%- set from_relation = adapter.base_relation_from_materialization_config(materialization) -%}
    {%- else -%}
        {%- set from_relation = materialization -%}
    {%- endif-%}
    {%- set to_relation = from_relation.incorporate(path={"identifier": new_name}) -%}

    {{- adapter.dispatch('rename_sql', 'dbt')(materialization, new_name) -}}

    {{- adapter.cache_renamed(from_relation, to_relation) -}}

{%- endmacro -%}


{%- macro default__rename_sql(materialization, new_name) -%}

    {%- set relation_type = materialization.type -%}

    {%- if relation_type == 'view' -%}
        {{ rename_view_sql(materialization, new_name) }}
    {%- elif relation_type == 'table' -%}
        {{ rename_table_sql(materialization, new_name) }}
    {%- elif relation_type == 'materialized_view' -%}
        {{ rename_materialized_view_sql(materialization, new_name) }}
    {%- else -%}
        {{- exceptions.raise_compiler_error("`rename_sql()` has not been implemented for the relation type" ~ relation_type ) -}}
    {%- endif -%}

{%- endmacro -%}
