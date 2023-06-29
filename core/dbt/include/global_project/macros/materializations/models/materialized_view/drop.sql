{%- macro drop_materialized_view_sql(materialized_view) -%}
    {{- adapter.dispatch('drop_materialized_view_sql', 'dbt')(materialized_view) -}}
{%- endmacro -%}


{%- macro default__drop_materialized_view_sql(materialized_view) -%}
    {{- exceptions.raise_compiler_error("`drop_materialized_view_sql()` has not been implemented for this adapter.") -}}
{%- endmacro -%}


-- This is the one that gets used when non-materialized-views call drop
{%- macro drop_materialized_view(relation) -%}

    {{- log('Applying DROP to: ' ~ relation) -}}

    {{- return(adapter.dispatch('drop_materialized_view', 'dbt')(relation)) -}}

    {{- adapter.cache.drop(relation) -}}

{%- endmacro %}


{%- macro default__drop_materialized_view(relation) -%}
    {{- exceptions.raise_compiler_error("`drop_materialized_view()` has not been implemented for this adapter.") -}}
{%- endmacro %}
