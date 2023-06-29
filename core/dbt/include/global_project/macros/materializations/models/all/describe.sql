{# /*
    This needs to be a {% do return(...) %} because the macro returns a dictionary, not a template.
*/ #}

{%- macro describe_sql(materialization) -%}

    {{- log('Applying DESCRIBE to: ' ~ materialization) -}}

    {%- do return(adapter.dispatch('describe_sql', 'dbt')(materialization)) -%}

{%- endmacro -%}


{%- macro default__describe_sql(materialization) -%}

    {%- set relation_type = materialization.type -%}

    {%- if relation_type == adapter.RelationType.MaterializedView -%}
        {%- do return(describe_materialized_view_sql(materialization)) -%}
    {%- else -%}
        {{- exceptions.raise_compiler_error("`describe_sql()` has not been implemented for the relation type" ~ relation_type ) -}}
    {%- endif -%}

{%- endmacro -%}
