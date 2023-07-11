{%- macro refresh_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying REFRESH to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('refresh_template', 'dbt')(relation) -}}
{%- endmacro -%}


{%- macro default__refresh_template(relation) -%}

    {%- if relation.type == 'materialized_view' -%}
        {{ refresh_materialized_view_template(relation) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`refresh_template` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}
