{%- macro stage_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying STAGE to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('stage_template', 'dbt')(relation) -}}
{%- endmacro -%}


{%- macro default__stage_template(relation) -%}

    -- get the standard intermediate name
    {% set intermediate_relation = adapter.relation_factory.make_intermediate(relation) -%}

    -- drop any pre-existing intermediate
    {{ drop_template(intermediate_relation, called_directly=False) }};

    {{ create_template(intermediate_relation, called_directly=False) }}

{%- endmacro -%}
