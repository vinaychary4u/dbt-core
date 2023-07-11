{% macro drop_index_sql(relation, index_name) -%}
    {{ adapter.dispatch('drop_index_sql', 'dbt')(relation, index_name) }}
{%- endmacro %}

{% macro default__drop_index_sql(relation, index_name) -%}
    {{ exceptions.raise_compiler_error("`drop_index_sql` has not been implemented for this adapter.") }}
{%- endmacro %}
