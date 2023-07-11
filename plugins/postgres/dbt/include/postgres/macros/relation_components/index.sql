{#- /*
    This file contains DDL that gets consumed in the Postgres implementation of the materialized view materialization.
    These macros could be used elsewhere as they do not care that they are being called by a materialization;
    but the original intention was to support the materialization of materialized views. These macros represent
    the basic interactions dbt-postgres requires of indexes in Postgres:
        - ALTER
        - CREATE
        - DESCRIBE
        - DROP
    These macros all take a `PostgresIndexRelation` instance and/or a `Relation` instance as an input.
    These classes can be found in the following files, respectively:
        `dbt/adapters/postgres/relation_configs/index.py`
        `dbt/adapters/relation/models/_relation.py`

    Used in:
        `dbt/include/postgres/macros/relations/materialized_view.sql`
    Uses:
        `dbt/adapters/postgres/relation/models/index.py`
        `dbt/adapters/postgres/relation/models/materialized_view.py`
*/ -#}


{% macro postgres__alter_indexes_template(relation, index_changeset) -%}
    {{- log('Applying ALTER INDEXES to: ' ~ relation) -}}

    {%- for _change in index_changeset -%}
        {%- set _index = _change.context -%}

        {% if _change.action == 'drop' -%}
            {{ postgres__drop_index_template(relation, _index) }};

        {% elif _change.action == 'create' -%}
            {{ postgres__create_index_template(relation, _index) }};

        {%- endif -%}
    {%- endfor -%}

{%- endmacro %}


{% macro postgres__create_indexes_template(relation) -%}

    {% for _index in relation.indexes -%}
        {{- postgres__create_index_template(relation, _index) -}}
        {%- if not loop.last %};{% endif -%}
    {%- endfor -%}

{%- endmacro %}


{% macro postgres__create_index_template(relation, index) -%}

    {%- set _index_name = adapter.generate_index_name(relation, index) -%}

    create {% if index.unique -%}unique{%- endif %} index if not exists "{{ _index_name }}"
        on {{ relation.fully_qualified_path }}
        using {{ index.method }}
        (
            {{ ", ".join(index.column_names) }}
        )

{%- endmacro %}


{% macro postgres__describe_indexes_template(relation) %}
    {%- if adapter.is_relation_model(relation) -%}
        {%- set _name = relation.name %}
        {%- set _schema = relation.schema_name %}
    {%- else -%}
        {%- set _name = relation.identifier %}
        {%- set _schema = relation.schema %}
    {%- endif -%}
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
    where t.relname ilike '{{ _name }}'
      and n.nspname ilike '{{ _schema }}'
      and t.relkind in ('r', 'm')
    group by 1, 2, 3
    order by 1, 2, 3
{% endmacro %}


{% macro postgres__drop_index_template(relation, index) -%}
    drop index if exists "{{ relation.schema_name }}"."{{ index.name }}" cascade
{%- endmacro %}


{# /*
    These are `BaseRelation` versions. The `BaseRelation` workflows are different.
*/ #}
{% macro postgres__get_create_index_sql(relation, index_dict) -%}
  {%- set index_config = adapter.parse_index(index_dict) -%}
  {%- set comma_separated_columns = ", ".join(index_config.columns) -%}
  {%- set index_name = index_config.render(relation) -%}

  create {% if index_config.unique -%}
    unique
  {%- endif %} index if not exists
  "{{ index_name }}"
  on {{ relation }} {% if index_config.type -%}
    using {{ index_config.type }}
  {%- endif %}
  ({{ comma_separated_columns }});
{%- endmacro %}


{%- macro postgres__get_drop_index_sql(relation, index_name) -%}
    drop index if exists "{{ index_name }}"
{%- endmacro -%}
