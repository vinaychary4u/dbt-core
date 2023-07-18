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


{% macro postgres__get_show_indexes_sql(relation) %}
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
    where t.relname = '{{ relation.identifier }}'
      and n.nspname = '{{ relation.schema }}'
      and t.relkind in ('r', 'm')
    group by 1, 2, 3
    order by 1, 2, 3
{% endmacro %}


{%- macro postgres__get_drop_index_sql(relation, index_name) -%}
    drop index if exists "{{ relation.schema }}"."{{ index_name }}"
{%- endmacro -%}
