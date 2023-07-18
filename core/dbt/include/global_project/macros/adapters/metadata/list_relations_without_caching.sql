{% macro list_relations_without_caching(schema_relation) %}
  {{ return(adapter.dispatch('list_relations_without_caching', 'dbt')(schema_relation)) }}
{% endmacro %}


{% macro default__list_relations_without_caching(schema_relation) %}
  {{ exceptions.raise_not_implemented(
    'list_relations_without_caching macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
