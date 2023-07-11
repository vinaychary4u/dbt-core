{# /*
    This needs to be a {% do return(...) %} because the macro returns a dictionary, not a template.
*/ #}

{%- macro describe_template(relation, called_directly=True) -%}
    {%- if called_directly -%}
        {{- log('Applying DESCRIBE to: ' ~ relation) -}}
    {%- endif -%}
    {%- do return(adapter.dispatch('describe_template', 'dbt')(relation)) -%}
{%- endmacro -%}


{%- macro default__describe_template(relation) -%}

    {%- if relation.type == 'materialized_view' -%}
        {%- do return(describe_materialized_view_template(relation)) -%}

    {%- else -%}
        {{- exceptions.raise_compiler_error("`describe_template` has not been implemented for: " ~ relation.type ) -}}

    {%- endif -%}

{%- endmacro -%}
