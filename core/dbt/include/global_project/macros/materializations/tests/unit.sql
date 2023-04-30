{%- materialization unit, default -%}

  {% set relations = [] %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set all_expected_column_names = get_columns_in_query(sql) %}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length ) > 0 else all_expected_column_names %}
  {% set unit_test_sql = get_unit_test_sql(sql, get_fixture_sql(expected_rows, all_expected_column_names), tested_expected_column_names) %}

  {% if should_store_failures() %}

    {% set identifier = model['alias'] %}
    {% set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) %}
    {% set target_relation = api.Relation.create(
        identifier=identifier, schema=schema, database=database, type='table') -%} %}

    {% if old_relation %}
        {% do adapter.drop_relation(old_relation) %}
    {% endif %}

    {% call statement(auto_begin=True) %}
        {{ create_table_as(False, target_relation, unit_test_sql) }}
    {% endcall %}

    {% do relations.append(target_relation) %}

    {% set main_sql %}
        select *
        from {{ target_relation }}
    {% endset %}

    {{ adapter.commit() }}

  {% else %}

      {% set main_sql = unit_test_sql %}

  {% endif %}

  {% call statement('main', fetch_result=True) -%}

    {{ main_sql }}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
