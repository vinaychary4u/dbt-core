{%- macro alter_template(existing_relation, target_relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying ALTER to: ' ~ existing_relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('alter_template', 'dbt')(existing_relation, target_relation) -}}
{%- endmacro -%}


{%- macro default__alter_template(existing_relation, target_relation) -%}

    {%- if existing_relation.type == 'materialized_view' -%}
        {{ alter_materialized_view_template(existing_relation, target_relation) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`alter_template` has not been implemented for: " ~ existing_relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}
