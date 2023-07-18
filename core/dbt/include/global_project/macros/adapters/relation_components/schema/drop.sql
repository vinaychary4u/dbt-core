{% macro drop_schema(relation) -%}
  {{ adapter.dispatch('drop_schema', 'dbt')(relation) }}
{% endmacro %}


{% macro default__drop_schema(relation) -%}
  {%- call statement('drop_schema') -%}
    drop schema if exists {{ relation.without_identifier() }} cascade
  {% endcall %}
{% endmacro %}
