{% macro list_schemas(database) -%}
  {{ return(adapter.dispatch('list_schemas', 'dbt')(database)) }}
{% endmacro %}


{% macro default__list_schemas(database) -%}
  {% set sql %}
    select distinct schema_name
    from {{ information_schema_name(database) }}.SCHEMATA
    where catalog_name ilike '{{ database }}'
  {% endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}


{% macro check_schema_exists(information_schema, schema) -%}
  {{ return(adapter.dispatch('check_schema_exists', 'dbt')(information_schema, schema)) }}
{% endmacro %}


{% macro default__check_schema_exists(information_schema, schema) -%}
  {% set sql -%}
        select count(*)
        from {{ information_schema.replace(information_schema_view='SCHEMATA') }}
        where catalog_name='{{ information_schema.database }}'
          and schema_name='{{ schema }}'
  {%- endset %}
  {{ return(run_query(sql)) }}
{% endmacro %}
