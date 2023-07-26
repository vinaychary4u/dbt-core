
{% macro get_relations() %}
  {{ return(adapter.dispatch('get_relations', 'dbt')()) }}
{% endmacro %}

{% macro default__get_relations() %}
  {{ exceptions.raise_not_implemented(
    'get_relations macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
