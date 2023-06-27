-- This is the new one that gets used with RelationConfigBase instances
{% macro drop_materialized_view_sql(materialized_view) -%}
    {{ return(adapter.dispatch('drop_materialized_view_sql', 'dbt')(materialized_view)) }}
{%- endmacro %}


{% macro default__drop_materialized_view_sql(materialized_view) -%}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{%- endmacro %}


-- This is the old one that gets used with BaseRelation instances
{% macro drop_materialized_view(relation) -%}
    {{ return(adapter.dispatch('drop_materialized_view', 'dbt')(relation)) }}
{%- endmacro %}


{% macro default__drop_materialized_view(relation) -%}
    {{ exceptions.raise_compiler_error("Materialized views have not been implemented for this adapter.") }}
{%- endmacro %}
