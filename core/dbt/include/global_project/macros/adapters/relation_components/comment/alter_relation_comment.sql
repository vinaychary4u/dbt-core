{% macro alter_relation_comment(relation, relation_comment) -%}
  {{ return(adapter.dispatch('alter_relation_comment', 'dbt')(relation, relation_comment)) }}
{% endmacro %}


{% macro default__alter_relation_comment(relation, relation_comment) -%}
  {{ exceptions.raise_not_implemented(
    'alter_relation_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
