{%- macro create_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying CREATE to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('create_template', 'dbt')(relation) -}}

    {{- adapter.cache_created_relation_model(relation) -}}
{%- endmacro -%}


{%- macro default__create_template(relation) -%}

    {%- if relation.type == 'materialized_view' -%}
        {{ create_materialized_view_template(relation) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`create_template` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}
