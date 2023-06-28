{#- /*
    This file contains DDL that gets consumed in the default materialized view materialization in `dbt-core`.
    These macros could be used elsewhere as they do not care that they are being called by a materialization;
    but the original intention was to support the materialization of materialized views. These macros represent
    the basic interactions dbt-postgres requires of materialized views in Postgres:
        - ALTER
        - CREATE
        - DESCRIBE
        - DROP
        - REFRESH
        - RENAME
        - REPLACE
    These macros all take a PostgresMaterializedViewConfig instance as an input. This class can be found in:
        `dbt/adapters/postgres/relation_configs/materialized_view.py`

    Used in:
        `dbt/include/global_project/macros/materializations/models/materialized_view/materialized_view.sql`
    Uses:
        `dbt/adapters/postgres/relation.py`
        `dbt/adapters/postgres/relation_configs/`
*/ -#}


{% macro postgres__alter_materialized_view_sql(new_materialized_view, existing_materialized_view) %}

    {#- /*
        We need to get the config changeset to determine if we require a full refresh (happens if any change
        in the changeset requires a full refresh or if an unmonitored change was detected)
        or if we can get away with altering the dynamic table in place.
    */ -#}
    {% set _config_changeset = adapter.Relation.materialized_view_config_changeset(new_materialized_view, existing_materialized_view) %}

    {% if _config_changeset.requires_full_refresh %}
        {{ replace_materialized_view_sql(new_materialized_view) }}

    {% else %}
        {{ postgres__alter_indexes_sql(new_materialized_view, _config_changeset.indexes) }}

    {%- endif -%}

{% endmacro %}


{% macro postgres__create_materialized_view_sql(materialized_view, as_intermediate=False) %}
    {%- if as_intermediate -%}
        {%- set materialized_view_path = materialized_view.fully_qualified_path_intermediate -%}
    {%- else -%}
        {%- set materialized_view_path = materialized_view.fully_qualified_path -%}
    {%- endif -%}

    create materialized view {{ materialized_view_path }} as
        {{ materialized_view.query }}
    ;

    {{- postgres__create_indexes_sql(materialized_view, on_intermediate) -}}

{% endmacro %}


{% macro postgres__describe_materialized_view_sql(materialized_view) %}

    {% set _materialized_view_sql -%}
        select
            v.matviewname,
            v.schemaname,
            '{{ this.database }}' as databasename,
            v.definition
        from pg_matviews v
        where v.matviewname ilike '{{ materialized_view.name }}'
          and v.schemaname ilike '{{ materialized_view.schema_name }}'
    {%- endset -%}
    {% set _materialized_view = run_query(_materialized_view_sql) %}

    {%- set _indexes_sql -%}
        select
            i.relname                                   as name,
            m.amname                                    as method,
            ix.indisunique                              as "unique",
            array_to_string(array_agg(a.attname), ',')  as column_names
        from pg_index ix
        join pg_class i
            on i.oid = ix.indexrelid
        join pg_am m
            on m.oid=i.relam
        join pg_class t
            on t.oid = ix.indrelid
        join pg_namespace n
            on n.oid = t.relnamespace
        join pg_attribute a
            on a.attrelid = t.oid
            and a.attnum = ANY(ix.indkey)
        where t.relname ilike '{{ materialized_view.name }}'
          and n.nspname ilike '{{ materialized_view.schema_name }}'
          and t.relkind = 'm'
        group by 1, 2, 3
        order by 1, 2, 3
    {%- endset -%}
    {% set _indexes = run_query(_indexes_sql) %}

    {% do return({'materialized_view': _materialized_view, 'indexes': _indexes}) %}

{% endmacro %}


-- This is the new one that gets used with RelationConfigBase instances
{% macro postgres__drop_materialized_view_sql(materialized_view) -%}
    drop materialized view if exists {{ materialized_view.fully_qualified_path }} cascade
{%- endmacro %}


-- This is the old one that gets used with BaseRelation instances
{% macro postgres__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }} cascade
{%- endmacro %}


{% macro postgres__refresh_materialized_view_sql(materialized_view) %}
    refresh materialized view {{ materialized_view.fully_qualified_path }}
{% endmacro %}


{% macro postgres__rename_materialized_view_sql(materialized_view, name, from_intermediate=False) %}
    {%- if from_intermediate -%}
        {%- set materialized_view_path = materialized_view.fully_qualified_path_intermediate -%}
    {%- else -%}
        {%- set materialized_view_path = materialized_view.fully_qualified_path -%}
    {%- endif -%}

    alter materialized view {{ materialized_view_path }} rename to {{ name }}
{% endmacro %}


{% macro postgres__replace_materialized_view_sql(new_materialized_view, existing_relation) %}
    {{- create_materialized_view_sql(new_materialized_view, as_intermediate=True) -}}
--TODO: this only works for existing materialized views
    {{ rename_materialized_view_sql(new_materialized_view, new_materialized_view.backup_name) }};
    {{ rename_materialized_view_sql(new_materialized_view, new_materialized_view.name, from_intermediate=True) }}
{% endmacro %}
