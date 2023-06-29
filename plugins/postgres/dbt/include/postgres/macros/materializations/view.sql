{# /*
    These are BaseRelation instances, not MaterializationConfig instances
*/ #}


{%- macro postgres__rename_view_sql(view, new_name) -%}
    alter view {{ view }} rename to {{ new_name }}
{%- endmacro -%}
