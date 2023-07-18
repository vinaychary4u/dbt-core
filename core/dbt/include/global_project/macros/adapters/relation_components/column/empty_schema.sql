{% macro get_empty_schema_sql(columns) -%}
  {{ return(adapter.dispatch('get_empty_schema_sql', 'dbt')(columns)) }}
{% endmacro %}


{% macro default__get_empty_schema_sql(columns) %}
    {%- set col_err = [] -%}
    select
    {% for i in columns %}
      {%- set col = columns[i] -%}
      {%- if col['data_type'] is not defined -%}
        {{ col_err.append(col['name']) }}
      {%- endif -%}
      {% set col_name = adapter.quote(col['name']) if col.get('quote') else col['name'] %}
      cast(null as {{ col['data_type'] }}) as {{ col_name }}{{ ", " if not loop.last }}
    {%- endfor -%}
    {%- if (col_err | length) > 0 -%}
      {{ exceptions.column_type_missing(column_names=col_err) }}
    {%- endif -%}
{% endmacro %}
