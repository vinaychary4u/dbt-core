{%- macro postgres__get_show_grant_sql(relation) -%}
  select grantee, privilege_type
  from {{ relation.information_schema('role_table_grants') }}
      where grantor = current_role
        and grantee != current_role
        and table_schema = '{{ relation.schema }}'
        and table_name = '{{ relation.identifier }}'
{%- endmacro -%}


{% macro postgres__copy_grants() %}
    {{ return(False) }}
{% endmacro %}
