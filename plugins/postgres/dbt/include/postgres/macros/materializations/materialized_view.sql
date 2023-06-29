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


{%- macro postgres__alter_materialized_view_sql(existing_materialized_view, new_materialized_view) -%}

    {#- /*
        We need to get the config changeset to determine if we require a full refresh (happens if any change
        in the changeset requires a full refresh or if an unmonitored change was detected)
        or if we can get away with altering the dynamic table in place.
    */ -#}
    {%- set _config_changeset = adapter.Materialization.materialized_view_config_changeset(existing_materialized_view, new_materialized_view) -%}

    {%- if _config_changeset.requires_full_refresh -%}
        {{ replace_materialized_view_sql(new_materialized_view) }}

    {%- else -%}
        {{ postgres__alter_indexes_sql(new_materialized_view, _config_changeset.indexes) }}

    {%- endif -%}

{%- endmacro -%}


{%- macro postgres__create_materialized_view_sql(materialized_view) -%}

    create materialized view {{ materialized_view.fully_qualified_path }} as
        {{ materialized_view.query }}
    ;

    {{ postgres__create_indexes_sql(materialized_view) -}}

{%- endmacro -%}


{%- macro postgres__describe_materialized_view_sql(materialized_view) -%}

    {%- set _materialized_view_sql -%}
        select
            v.matviewname,
            v.schemaname,
            '{{ this.database }}' as databasename,
            v.definition
        from pg_matviews v
        where v.matviewname ilike '{{ materialized_view.name }}'
          and v.schemaname ilike '{{ materialized_view.schema_name }}'
    {%- endset -%}
    {%- set _materialized_view = run_query(_materialized_view_sql) -%}

    {%- set _indexes_sql = postgres__describe_indexes_sql(materialized_view) -%}
    {%- set _indexes = run_query(_indexes_sql) -%}

    {%- do return({'materialized_view': _materialized_view, 'indexes': _indexes}) -%}

{%- endmacro -%}


{%- macro postgres__drop_materialized_view_sql(materialized_view) -%}
    drop materialized view if exists {{ materialized_view.fully_qualified_path }} cascade
{%- endmacro -%}


-- This is the old one that gets used with BaseRelation instances
{%- macro postgres__drop_materialized_view(relation) -%}
    drop materialized view if exists {{ relation }} cascade
{%- endmacro -%}


{%- macro postgres__refresh_materialized_view_sql(materialized_view) -%}
    refresh materialized view {{ materialized_view.fully_qualified_path }}
{%- endmacro -%}


{%- macro postgres__rename_materialized_view_sql(materialized_view, new_name) -%}

    {%- if adapter.is_materialization_config(materialized_view) -%}
        {%- set fully_qualified_path = materialized_view.fully_qualified_path -%}
    {%- else -%}
        {%- set fully_qualified_path = materialized_view -%}
    {%- endif -%}

    alter materialized view {{ fully_qualified_path }} rename to {{ new_name }}

{%- endmacro -%}
