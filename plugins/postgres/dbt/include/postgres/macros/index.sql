{#- /*
    This file contains DDL that gets consumed in the Postgres implementation of the materialized view materialization
    in `dbt/include/postgres/macros/materializations/materialized_view.sql`.
    These macros could be used elsewhere as they do not care that they are being called by a materialization;
    but the original intention was to support the materialization of materialized views. These macros represent
    the basic interactions dbt-postgres requires of indexes in Postgres:
        - ALTER
        - CREATE
        - DROP
    These macros all take a PostgresIndexConfig instance and/or a MaterializationConfigBase as an input.
    These classes can be found in the following files, respectively:
        `dbt/adapters/postgres/relation_configs/index.py`
        `dbt/adapters/relation_configs/materialization.py`

    Used in:
        `dbt/include/postgres/macros/materializations/materialized_view.sql`
    Uses:
        `dbt/adapters/postgres/relation_configs/index.py`
        `dbt/adapters/postgres/relation_configs/materialized_view.py`
*/ -#}


{% macro postgres__alter_indexes_sql(materialization_config, index_changeset) -%}
    {{- log('Applying UPDATE INDEXES to: ' ~ materialization_config.fully_qualified_path) -}}

    {%- for _index_change in index_changeset -%}
        {%- set _index_config = _index_change.context -%}

        {%- if _index_change.action == adapter.relation_config_change_action.drop -%}
            {{ postgres__drop_index_sql(_index_config) }};

        {%- elif _index_change.action == adapter.relation_config_change_action.create -%}
            {{ postgres__create_index_sql(materialization_config, _index_config) }};

        {%- endif -%}
    {%- endfor -%}

{%- endmacro %}


{% macro postgres__create_indexes_sql(materialization_config) -%}

    {% for _index_config in materialization_config.indexes -%}
        {{- postgres__create_index_sql(materialization_config, _index_config) -}};
    {%- endfor -%}

{%- endmacro %}


{% macro postgres__create_index_sql(materialization_config, index_config) -%}

    {%- set _index_name = adapter.generate_index_name(materialization_config, index_config) -%}

    create {% if index_config.unique -%}unique{%- endif %} index if not exists "{{ _index_name }}"
        on {{ materialization_config.fully_qualified_path }}
        using {{ index_config.method }}
        (
            {{ ", ".join(index_config.column_names) }}
        )

{%- endmacro %}


{% macro postgres__drop_index_sql(index_config) -%}
    drop index if exists "{{ index_config.name }}" cascade;
{%- endmacro %}
