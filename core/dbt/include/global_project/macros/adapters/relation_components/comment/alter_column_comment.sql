{% macro alter_column_comment(relation, column_dict) -%}
  {{ return(adapter.dispatch('alter_column_comment', 'dbt')(relation, column_dict)) }}
{% endmacro %}


{% macro default__alter_column_comment(relation, column_dict) -%}
  {{ exceptions.raise_not_implemented(
    'alter_column_comment macro not implemented for adapter '+adapter.type()) }}
{% endmacro %}
