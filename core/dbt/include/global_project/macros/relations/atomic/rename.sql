{%- macro rename_template(relation, new_name, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying RENAME to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('rename_template', 'dbt')(relation, new_name) -}}

    {{- adapter.cache_renamed_relation_model(relation, new_name) -}}
{%- endmacro -%}


{%- macro default__rename_template(relation, new_name) -%}

    {%- if relation.type == 'view' -%}
        {{ rename_view_template(relation, new_name) }}

    {%- elif relation.type == 'table' -%}
        {{ rename_table_template(relation, new_name) }}

    {%- elif relation.type == 'materialized_view' -%}
        {{ rename_materialized_view_template(relation, new_name) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`rename_template` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}


{# /*
    These are `BaseRelation` versions. The `BaseRelation` workflows are different.
*/ #}
{% macro rename_relation(from_relation, to_relation) -%}
  {{ return(adapter.dispatch('rename_relation', 'dbt')(from_relation, to_relation)) }}
{% endmacro %}

{% macro default__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% call statement('rename_relation') -%}
    alter table {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}
