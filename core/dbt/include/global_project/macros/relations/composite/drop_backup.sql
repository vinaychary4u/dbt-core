{%- macro drop_backup_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying DROP BACKUP to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('drop_backup_template', 'dbt')(relation) -}}
{%- endmacro -%}


{%- macro default__drop_backup_template(relation) -%}

    -- get the standard backup name
    {% set backup_relation_stub = adapter.relation_factory.make_backup_stub(relation) -%}

    {{ drop_template(backup_relation_stub, called_directly=False) }}

{%- endmacro -%}
