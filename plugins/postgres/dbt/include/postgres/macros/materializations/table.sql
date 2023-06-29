{# /*
    These are BaseRelation instances, not MaterializationConfig instances
*/ #}


{%- macro postgres__rename_table_sql(table, new_name) -%}
    alter table {{ table }} rename to {{ new_name }}
{%- endmacro -%}
