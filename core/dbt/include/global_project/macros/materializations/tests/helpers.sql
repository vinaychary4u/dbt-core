{% macro get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
  {{ adapter.dispatch('get_test_sql', 'dbt')(main_sql, fail_calc, warn_if, error_if, limit) }}
{%- endmacro %}

{% macro default__get_test_sql(main_sql, fail_calc, warn_if, error_if, limit) -%}
    select
      {{ fail_calc }} as failures,
      {{ fail_calc }} {{ warn_if }} as should_warn,
      {{ fail_calc }} {{ error_if }} as should_error
    from (
      {{ main_sql }}
      {{ "limit " ~ limit if limit != none }}
    ) dbt_internal_test
{%- endmacro %}


{% macro get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}
  {{ adapter.dispatch('get_unit_test_sql', 'dbt')(main_sql, expected_fixture_sql, expected_column_names) }}
{%- endmacro %}

{% macro default__get_unit_test_sql(main_sql, expected_fixture_sql, expected_column_names) -%}
    select
      {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%},{% endif %}{%- endfor -%}, "actual" as actual_or_expected
    from (
      {{ main_sql }}
    ) dbt_internal_unit_test_actual
    union all
    select
      {% for expected_column_name in expected_column_names %}{{expected_column_name}}{% if not loop.last -%},{% endif %}{%- endfor -%}, "expected" as actual_or_expected
    from (
      {{ expected_fixture_sql }}
    ) dbt_internal_unit_test_expected
{%- endmacro %}
