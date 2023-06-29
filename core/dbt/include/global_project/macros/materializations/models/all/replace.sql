{# /*
    This macro does dispatch to a relation_type-specific macro because it's actually a composite.
*/ #}

{%- macro replace_sql(existing_materialization, new_materialization) -%}

    {{- log('Applying REPLACE to: ' ~ existing_materialization) -}}

    {{- adapter.dispatch('replace_sql', 'dbt')(existing_materialization, new_materialization) -}}

{%- endmacro -%}


{%- macro default__replace_sql(existing_materialization, new_materialization) -%}
    {{- exceptions.raise_compiler_error("`replace_sql` has not been implemented for this adapter.") -}}
{%- endmacro -%}
