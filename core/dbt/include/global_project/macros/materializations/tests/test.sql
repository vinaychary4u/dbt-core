{%- materialization test, default -%}

  {% set relations = [] %}
    {% set relation_type = config.get('strategy') %}

    {% if relation_type in ['view', 'table'] %}

        {% set identifier = model['alias'] %}
        {% set existing_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
        {% set target_relation = api.Relation.create(
            database=database, schema=schema, identifier=identifier, type=relation_type
        ) %}

        {% call statement(auto_begin=True) %}
            {% if existing_relation %}
                {{ get_replace_sql(existing_relation, target_relation, sql) }}
            {% else %}
                {{ get_create_sql(target_relation, sql) }}
            {% endif %}
        {% endcall %}

        {% do relations.append(target_relation) %}

        {% set main_sql %}
            select * from {{ target_relation }}
        {% endset %}

        {{ adapter.commit() }}

  {% elif should_store_failures() %}

    {% set identifier = model['alias'] %}
    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
    {% set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type='table') -%} %}

    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    {% call statement(auto_begin=True) %}
        {{ create_table_as(False, target_relation, sql) }}
    {% endcall %}

    {% do relations.append(target_relation) %}

    {% set main_sql %}
        select *
        from {{ target_relation }}
    {% endset %}

    {{ adapter.commit() }}

  {% else %}

      {% set main_sql = sql %}

  {% endif %}

  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}

  {% call statement('main', fetch_result=True) -%}

    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit)}}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
