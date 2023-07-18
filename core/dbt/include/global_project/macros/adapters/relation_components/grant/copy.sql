{#
  -- COPY GRANTS
  -- When a relational object (view or table) is replaced in this database,
  -- do previous grants carry over to the new object? This may depend on:
  --    whether we use alter-rename-swap versus CREATE OR REPLACE
  --    user-supplied configuration (e.g. copy_grants on Snowflake)
  -- By default, play it safe, assume TRUE: that grants ARE copied over.
  -- This means dbt will first "show" current grants and then calculate diffs.
  -- It may require an additional query than is strictly necessary,
  -- but better safe than sorry.
#}

{% macro copy_grants() %}
    {{ return(adapter.dispatch('copy_grants', 'dbt')()) }}
{% endmacro %}


{% macro default__copy_grants() %}
    {{ return(True) }}
{% endmacro %}
