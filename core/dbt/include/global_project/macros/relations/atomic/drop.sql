{%- macro drop_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying DROP to: ' ~ relation) -}}
    {%- endif -%}
    {{- adapter.dispatch('drop_template', 'dbt')(relation) -}}

    {{- adapter.cache_dropped_relation_model(relation) -}}
{%- endmacro -%}


{%- macro default__drop_template(relation) -%}

    {%- if relation.type == 'view' -%}
        {{ drop_view_template(relation) }}

    {%- elif relation.type == 'table' -%}
        {{ drop_table_template(relation) }}

    {%- elif relation.type == 'materialized_view' -%}
        {{ drop_materialized_view_template(relation) }}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`drop_template` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}


{# /*
    These are `BaseRelation` versions. The `BaseRelation` workflows are different.
*/ #}
{% macro drop_relation_if_exists(relation) %}
  {% if relation is not none %}
    {{ adapter.drop_relation(relation) }}
  {% endif %}
{% endmacro %}


{% macro drop_relation(relation) -%}
    {{ return(adapter.dispatch('drop_relation', 'dbt')(relation)) }}
{% endmacro %}

{% macro default__drop_relation(relation) -%}
    {% call statement('drop_relation', auto_begin=False) -%}

        {%- if relation.is_view -%}
            drop view if exists {{ relation }} cascade

        {%- elif relation.is_table -%}
            drop table if exists {{ relation }} cascade

        {%- elif relation.is_materialized_view -%}
            drop materialized view if exists {{ relation }} cascade

        {%- else -%}
            drop {{ relation.type }} if exists {{ relation }} cascade

        {%- endif -%}
    {%- endcall %}
{% endmacro %}
