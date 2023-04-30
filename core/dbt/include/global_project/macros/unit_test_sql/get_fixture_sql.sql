{% macro get_fixture_sql(rows, column_names) %}

{% set default_row = {} %}
{%- for column_name in column_names -%}
    {%- do default_row.update({column_name: "null"}) -%}
{%- endfor -%}

{% for row in rows -%}
{%- set default_row_copy = default_row.copy() -%}
{%- do default_row_copy.update(row) -%}
SELECT
{%- for column_name, column_value in default_row_copy.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%},{%- endif %}
{%- endfor %}
{%- if not loop.last %}
UNION ALL
{% endif %}
{%- endfor %}


{%- if (rows | length) == 0  %}
SELECT
{%- for column_name, column_value in default_row.items() %} {{ column_value }} AS {{ column_name }}{% if not loop.last -%},{%- endif %}
{%- endfor %}
LIMIT 0
{% endif %}

{% endmacro %}
