{% macro get_column_schema_from_query(select_sql, select_sql_header=none) -%}
    {% set columns = [] %}
    {# -- Using an 'empty subquery' here to get the same schema as the given select_sql statement, without necessitating a data scan.#}
    {% set sql = get_empty_subquery_sql(select_sql, select_sql_header) %}
    {% set column_schema = adapter.get_column_schema_from_query(sql) %}
    {{ return(column_schema) }}
{% endmacro %}


{% macro get_empty_subquery_sql(select_sql, select_sql_header=none) -%}
  {{ return(adapter.dispatch('get_empty_subquery_sql', 'dbt')(select_sql, select_sql_header)) }}
{% endmacro %}


{#
  Builds a query that results in the same schema as the given select_sql statement, without necessitating a data scan.
  Useful for running a query in a 'pre-flight' context, such as model contract enforcement (assert_columns_equivalent macro).
#}
{% macro default__get_empty_subquery_sql(select_sql, select_sql_header=none) %}
    {%- if select_sql_header is not none -%}
    {{ select_sql_header }}
    {%- endif -%}
    select * from (
        {{ select_sql }}
    ) as __dbt_sbq
    where false
    limit 0
{% endmacro %}


-- here for back compat
{% macro get_columns_in_query(select_sql) -%}
  {{ return(adapter.dispatch('get_columns_in_query', 'dbt')(select_sql)) }}
{% endmacro %}


{% macro default__get_columns_in_query(select_sql) %}
    {% call statement('get_columns_in_query', fetch_result=True, auto_begin=False) -%}
        {{ get_empty_subquery_sql(select_sql) }}
    {% endcall %}
    {{ return(load_result('get_columns_in_query').table.columns | map(attribute='name') | list) }}
{% endmacro %}
