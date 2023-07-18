{% macro get_show_grant_sql(relation) %}
    {{ return(adapter.dispatch("get_show_grant_sql", "dbt")(relation)) }}
{% endmacro %}


{% macro default__get_show_grant_sql(relation) %}
    show grants on {{ relation }}
{% endmacro %}
