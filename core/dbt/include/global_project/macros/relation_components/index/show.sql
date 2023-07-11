{% macro show_indexes_sql(relation) -%}
    {{ adapter.dispatch('show_indexes_sql', 'dbt')(relation) }}
{%- endmacro %}

{% macro default__show_indexes_sql(relation) -%}
    {{ exceptions.raise_compiler_error("`show_indexes_sql` has not been implemented for this adapter.") }}
{%- endmacro %}
