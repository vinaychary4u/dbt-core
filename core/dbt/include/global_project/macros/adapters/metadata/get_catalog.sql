{% macro get_catalog(information_schema, schemas) -%}
  {{ return(adapter.dispatch('get_catalog', 'dbt')(information_schema, schemas)) }}
{%- endmacro %}


{% macro default__get_catalog(information_schema, schemas) -%}

  {% set typename = adapter.type() %}
  {% set msg -%}
    get_catalog not implemented for {{ typename }}
  {%- endset %}

  {{ exceptions.raise_compiler_error(msg) }}
{% endmacro %}
