{%- materialization unit, default -%}

  {% set relations = [] %}

  {% set expected_rows = config.get('expected_rows') %}
  {% set tested_expected_column_names = expected_rows[0].keys() if (expected_rows | length ) > 0 else get_columns_in_query(sql) %} %}
  
  {% set unit_test_sql = get_unit_test_sql(sql, get_expected_sql(expected_rows), tested_expected_column_names) %}

  {% call statement('main', fetch_result=True) -%}

    {{ unit_test_sql }}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
