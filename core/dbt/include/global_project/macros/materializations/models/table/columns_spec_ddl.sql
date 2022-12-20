{%- macro get_columns_spec_ddl() -%}
  {{ adapter.dispatch('get_columns_spec_ddl', 'dbt')() }}
{%- endmacro -%}

{% macro default__get_columns_spec_ddl() -%}
  {{ return(columns_spec_ddl()) }}
{%- endmacro %}

{% macro columns_spec_ddl() %}
  {# loop through user_provided_columns to create DDL with data types and constraints #}
  {% if config.get('constraints_enabled', False) %}
    (
    {% for i in model['columns'] %}
      {% set col = model['columns'][i] %}
      {% set constraints = col['constraints'] %}
      {% set constraints_check = col['constraints_check'] %}
      {{ col['name'] }} {{ col['data_type'] }} {% for x in constraints %} {{ x or "" }} {% endfor %} {% if constraints_check -%} check {{ constraints_check or "" }} {%- endif %} {{ "," if not loop.last }}
    {% endfor %}
  )
  {% endif %}
{% endmacro %}
