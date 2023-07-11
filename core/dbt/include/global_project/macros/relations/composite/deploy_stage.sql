{%- macro deploy_stage_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying DEPLOY STAGE to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('deploy_stage_template', 'dbt')(relation) -}}
{%- endmacro -%}


{%- macro default__deploy_stage_template(relation) -%}

    -- get the standard intermediate name
    {% set intermediate_relation = adapter.relation_factory.make_intermediate(relation) -%}

    {{ rename_template(intermediate_relation, relation.name, called_directly=False) }}

{%- endmacro -%}
