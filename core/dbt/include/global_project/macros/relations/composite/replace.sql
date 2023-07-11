{%- macro replace_template(existing_relation, target_relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying REPLACE to: ' ~ target_relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('replace_template', 'dbt')(existing_relation, target_relation) -}}
{%- endmacro -%}

{%- macro default__replace_template(existing_relation, target_relation) -%}

    {# /* create target_relation as an intermediate relation, then swap it out with the existing one using a backup */ #}
    {%- if target_relation.can_be_renamed and existing_relation.can_be_renamed -%}
        {{ stage_template(target_relation, called_directly=False) }};
        {{ backup_template(existing_relation, called_directly=False) }};
        {{ deploy_stage_template(target_relation, called_directly=False) }};
        {{ drop_backup_template(existing_relation, called_directly=False) }}

    {# /* create target_relation as an intermediate relation, then swap it out with the existing one using drop */ #}
    {%- elif target_relation.can_be_renamed -%}
        {{ stage_template(target_relation, called_directly=False) }};
        {{ drop_template(existing_relation, called_directly=False) }};
        {{ deploy_stage_template(target_relation, called_directly=False) }}

    {# /* create target_relation in place by first backing up the existing relation */ #}
    {%- elif existing_relation.can_be_renamed -%}
        {{ backup_template(existing_relation, called_directly=False) }};
        {{ create_template(target_relation, called_directly=False) }};
        {{ drop_backup_template(existing_relation, called_directly=False) }}

    {# /* no renaming is allowed, so just drop and create */ #}
    {%- else -%}
        {{ drop_template(existing_relation, called_directly=False) }};
        {{ create_template(target_relation, called_directly=False) }}

    {%- endif -%}

{%- endmacro -%}


{%- macro default__replace_sql_alt(existing_relation, target_relation) -%}

    {# /* stage the target relation if we can, otherwise we'll create it later */ #}
    {%- if target_relation.can_be_renamed -%}
        {{ stage_template(target_relation, called_directly=False) }};
    {%- endif -%}

    {# /* backup the existing relation if we can, otherwise just drop it */ #}
    {%- if existing_relation.can_be_renamed -%}
        {{ backup_template(existing_relation, called_directly=False) }};
    {%- else -%}
        {{ drop_template(existing_relation, called_directly=False) }};
    {%- endif -%}

    {# /* create the target relation from the staged relation if we were able to stage it earlier, otherwise create it here  */ #}
    {%- if target_relation.can_be_renamed -%}
        {{ deploy_stage_template(target_relation, called_directly=False) }}
    {%- else -%}
        {{ create_template(target_relation, called_directly=False) }}
    {%- endif -%}

    {# /* drop the backup relation if we were able to create it earlier */ #}
    {%- if existing_relation.can_be_renamed -%}
        ; -- we need this here because we don't know if the last statement happens in the previous if block until here
        {{ drop_backup_template(existing_relation, called_directly=False) }}
    {%- endif -%}

{%- endmacro -%}
