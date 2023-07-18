{% macro get_drop_index_sql(relation, index_name) -%}
    {{ adapter.dispatch('get_drop_index_sql', 'dbt')(relation, index_name) }}
{%- endmacro %}


{% macro default__get_drop_index_sql(relation, index_name) -%}
    {{ exceptions.raise_compiler_error("`get_drop_index_sql has not been implemented for this adapter.") }}
{%- endmacro %}
