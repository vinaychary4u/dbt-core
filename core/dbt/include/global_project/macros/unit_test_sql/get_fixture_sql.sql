{% macro get_fixture_sql(rows, columns) %}
-- Fixture for {{ model.name }}
{% set default_row = {} %}

{%- if not columns -%}
{%- set columns_in_relation = adapter.get_columns_in_relation(this) -%}
{%- set columns = [] -%}
{%- for column in columns_in_relation -%}
{%- do columns.append({"name": column.name, "data_type": column.dtype}) -%}
{%- endfor -%}
{%- endif -%}

{%- if not columns -%}
    {{ exceptions.raise_compiler_error("columns not available for" ~ model.name) }}
{%- endif -%}

{%- for column in columns -%}
    {%- do default_row.update({column["name"]: (safe_cast("null", column["data_type"]) | trim )}) -%}
{%- endfor -%}

{%- for row in rows -%}
{%- set default_row_copy = default_row.copy() -%}
{%- do default_row_copy.update(row) -%}
select
{%- for column_name, column_value in default_row_copy.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%}, {%- endif %}
{%- endfor %}
{%- if not loop.last %}
union all
{% endif %}
{%- endfor -%}

{%- if (rows | length) == 0 -%}
    select
    {%- for column_name, column_value in default_row.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%},{%- endif %}
    {%- endfor %}
    limit 0
{%- endif -%}
{% endmacro %}


{% macro get_expected_sql(rows) %}

{%- for row in rows -%}
select
{%- for column_name, column_value in row.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%}, {%- endif %}
{%- endfor %}
{%- if not loop.last %}
union all
{% endif %}
{%- endfor -%}

{%- if (rows | length) == 0 -%}
    select * FROM dbt_internal_unit_test_actual
    limit 0
{%- endif -%}

{% endmacro %}