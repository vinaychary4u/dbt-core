{%- materialization test, default -%}

  {% set relations = [] %}

  -- default SQL to use for the relation to test; will be modified depending on materialization_type
  {% set main_sql = sql %}

  -- default value
  {% set materialization_type = "ephemeral" %}
  {{ log("materialization_type: " ~ materialization_type ~ " (default)", True) }}

  -- default value if storing failures
  {% if should_store_failures() %}
      {% set materialization_type = "table" %}
  {% endif %}
  {{ log("materialization_type: " ~ materialization_type ~ " (after considering should_store_failures())", True) }}

  -- override the default (but only if configured to do so)
  {% set materialization_type = config.get("materialized") or materialization_type %}
  {{ log("materialization_type: " ~ materialization_type ~ " (after considering test config())", True) }}

  -- only allow certain materializations for now
  {% if materialization_type not in ["test", "ephemeral", "table", "view", "materialized_view"] %}
    {{ exceptions.raise_compiler_error("Invalid `materialization_type`. Got: " ~ materialization_type) }}
  {% endif %}

  -- only a few of the allowed materializations actually create database objects
  {% if materialization_type in ["table", "view", "materialized_view"] %}
      {%- set target_relation = this.incorporate(type=materialization_type) -%}
      {%- set materialization_macro = get_materialization_macro(materialization_type) -%}
      {% set relations = materialization_macro() %}

      {% set main_sql %}
          select *
          from {{ target_relation }}
      {% endset %}
  {% endif %}

  {% set limit = config.get('limit') %}
  {% set fail_calc = config.get('fail_calc') %}
  {% set warn_if = config.get('warn_if') %}
  {% set error_if = config.get('error_if') %}

  {{ log("main_sql: " ~ main_sql, True) }}

  {% call statement('main', fetch_result=True) -%}

    {{ get_test_sql(main_sql, fail_calc, warn_if, error_if, limit)}}

  {%- endcall %}

  {{ return({'relations': relations}) }}

{%- endmaterialization -%}
