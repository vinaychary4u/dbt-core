{%- macro postgres__drop_view_template(view) -%}
    drop view if exists {{ view.fully_qualified_path }} cascade
{%- endmacro -%}


{%- macro postgres__rename_view_template(view, new_name) -%}
    alter view {{ view.fully_qualified_path }} rename to {{ new_name }}
{%- endmacro -%}
