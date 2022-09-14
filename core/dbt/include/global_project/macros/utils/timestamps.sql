/* {#
Future work:
- Reconcile these with `date_function` and `snapshot_get_time`
#} */

---- CURRENT_TIMESTAMP ----

{% macro current_timestamp() -%}
  {{ adapter.dispatch('current_timestamp', 'dbt')() }}
{%- endmacro %}

{% macro default__current_timestamp() -%}
  {{ exceptions.raise_not_implemented(
    'current_timestamp macro not implemented for adapter '+adapter.type()) }}
{%- endmacro %}


---- CONVERT_TIMEZONE ----

-- {# Borrowed from https://github.com/dbt-labs/snowplow/blob/main/macros/adapters/convert_timezone.sql #}
{%- macro convert_timezone(in_tz, out_tz, in_timestamp) -%}
    {{ adapter.dispatch('convert_timezone', 'dbt')(in_tz, out_tz, in_timestamp) }}
{%- endmacro -%}

{% macro default__convert_timezone(in_tz, out_tz, in_timestamp) %}
    {% if in_tz is none %}
        convert_timezone({{out_tz}}, {{in_timestamp}})
    {% else %}
        convert_timezone({{in_tz}}, {{out_tz}}, {{in_timestamp}})
    {% endif %}
{% endmacro %}

-- move to dbt-postgres
{% macro postgres__convert_timezone(in_tz, out_tz, in_timestamp) %}
    {% if in_tz is none %}
        ({{in_timestamp}} at time zone {{out_tz}})::timestamptz
        convert_timezone({{out_tz}}, {{in_timestamp}})
    {% else %}
        ({{in_timestamp}} at time zone {{in_tz}} at time zone {{out_tz}})::timestamptz
    {% endif %}
{% endmacro %}

-- {# redshift should use default instead of postgres #}
-- TODO move to dbt-redshift
{% macro redshift__convert_timezone() %}
    {{ return(dbt.default__convert_timezone()) }}
{% endmacro %}


---- CURRENT_TIMESTAMP_IN_UTC ----


{% macro current_timestamp_in_utc() -%}
    {{ return(adapter.dispatch('current_timestamp_in_utc', 'dbt')()) }}
{%- endmacro %}

{% macro default__current_timestamp_in_utc() %}
    {{ return(
        convert_timezone(
            in_tz = none,
            out_tz = 'UTC',
            in_timestamp = current_timestamp()
      )) }}
{% endmacro %}

---------------------------------------------

/* {#
    DEPRECATED: DO NOT USE IN NEW PROJECTS
    
    This is ONLY to handle the fact that Snowflake + Postgres had functionally
    different implementations of {{ dbt.current_timestamp }} + {{ dbt_utils.current_timestamp }}
    
    If you had a project or package that called {{ dbt_utils.current_timestamp() }}, you should
    continue to use this macro to guarantee identical behavior on those two databases.
#} */

{% macro current_timestamp_backcompat() %}
    {{ return(adapter.dispatch('current_timestamp_backcompat', 'dbt')()) }}
{% endmacro %}

{% macro default__current_timestamp_backcompat() %}
    {{ return(adapter.dispatch('current_timestamp', 'dbt')()) }}
{% endmacro %}

-- TODO move to dbt-postgres
{% macro postgres__current_timestamp_backcompat() %}
    current_timestamp::{{ type_timestamp() }}
{% endmacro %}

-- TODO move to dbt-snowflake
{% macro snowflake__current_timestamp_backcompat() %}
    current_timestamp::{{ type_timestamp() }}
{% endmacro %}
