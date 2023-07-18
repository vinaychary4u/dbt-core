{% macro get_revoke_sql(relation, privilege, grantees) %}
    {{ return(adapter.dispatch('get_revoke_sql', 'dbt')(relation, privilege, grantees)) }}
{% endmacro %}


{%- macro default__get_revoke_sql(relation, privilege, grantees) -%}
    revoke {{ privilege }} on {{ relation }} from {{ grantees | join(', ') }}
{%- endmacro -%}


{% macro should_revoke(existing_relation, full_refresh_mode=True) %}

    {% if not existing_relation %}
        {#-- The table doesn't already exist, so no grants to copy over --#}
        {{ return(False) }}
    {% elif full_refresh_mode %}
        {#-- The object is being REPLACED -- whether grants are copied over depends on the value of user config --#}
        {{ return(copy_grants()) }}
    {% else %}
        {#-- The table is being merged/upserted/inserted -- grants will be carried over --#}
        {{ return(True) }}
    {% endif %}

{% endmacro %}
