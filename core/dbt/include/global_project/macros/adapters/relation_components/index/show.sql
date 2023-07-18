{% macro get_show_indexes_sql(relation) -%}
    {{ adapter.dispatch('get_show_indexes_sql', 'dbt')(relation) }}
{%- endmacro %}


{% macro default__get_show_indexes_sql(relation) -%}
    {{ exceptions.raise_compiler_error("`get_show_indexes_sql has not been implemented for this adapter.") }}
{%- endmacro %}
