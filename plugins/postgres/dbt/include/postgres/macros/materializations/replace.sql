{%- macro postgres__replace_sql(existing_materialization, new_materialization) -%}

    {#- new_materialization is always a MaterializationConfig instance -#}
    {%- set intermediate_materialization = adapter.make_intermediate_materialization_config(new_materialization) -%}
    {%- set new_name = new_materialization.name -%}


    {%- if adapter.is_materialization_config(existing_materialization) -%}
        {%- set backup_name = adapter.make_backup_name(existing_materialization) -%}

    {%- elif adapter.is_base_relation(existing_materialization) -%}
        {%- set backup_relation = make_backup_relation(existing_materialization, existing_materialization.type) -%}
        {%- set backup_name = backup_relation.identifier -%}

    {%- else -%}
        {{- exceptions.raise_compiler_error("Unexpected materialization type: " ~ existing_materialization) -}}

    {%- endif -%}


    {{ create_sql(intermediate_materialization) }};
    {{ rename_sql(existing_materialization, backup_name) }};
    {{ rename_sql(intermediate_materialization, new_name) }}

{%- endmacro -%}
