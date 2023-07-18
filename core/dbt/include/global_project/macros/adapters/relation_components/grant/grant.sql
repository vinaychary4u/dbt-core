{% macro get_grant_sql(relation, privilege, grantees) %}
    {{ return(adapter.dispatch('get_grant_sql', 'dbt')(relation, privilege, grantees)) }}
{% endmacro %}


{%- macro default__get_grant_sql(relation, privilege, grantees) -%}
    grant {{ privilege }} on {{ relation }} to {{ grantees | join(', ') }}
{%- endmacro -%}
