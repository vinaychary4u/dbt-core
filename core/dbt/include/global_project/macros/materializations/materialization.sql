{# /*
    These are step macros for building out a materialization based on `MaterializationConfig`, which can be
    found in `dbt/adapters/relation_configs/materialization.py`

    Note: You cannot start the macro name with "materialization" because then dbt thinks it's a materialization
    and not a macro.
*/ #}


{%- macro mat_setup(materialization_config, pre_hooks) -%}

    -- backup_relation and intermediate_relation should not already exist in the database
    -- it's possible these exist because of a previous run that exited unexpectedly
    {%- set backup_relation = adapter.get_cached_backup_relation_from_materialization_config(materialization_config) -%}
    {%- set intermediate_relation = adapter.get_cached_intermediate_relation_from_materialization_config(materialization_config) -%}

    -- drop the temp relations if they exist already in the database
    {{- drop_relation_if_exists(backup_relation) -}}
    {{- drop_relation_if_exists(intermediate_relation) -}}

    {{- run_hooks(pre_hooks, inside_transaction=False) -}}

{%- endmacro -%}


{%- macro mat_teardown(materialization_config, post_hooks) -%}

    -- backup_relation and intermediate_relation may exist if the materialized view was replaced
    {%- set backup_relation = adapter.get_cached_backup_relation_from_materialization_config(materialization_config) -%}
    {%- set intermediate_relation = adapter.get_cached_intermediate_relation_from_materialization_config(materialization_config) -%}

    -- drop the temp relations if they exist to leave the database clean for the next run
    {{- drop_relation_if_exists(backup_relation) -}}
    {{- drop_relation_if_exists(intermediate_relation) -}}

    {{- run_hooks(post_hooks, inside_transaction=False) -}}

{%- endmacro -%}


{%- macro mat_execute_no_op(materialization_config) -%}
    {%- do store_raw_result(
        name="main",
        message="skip " ~ materialization_config.fully_qualified_path,
        code="skip",
        rows_affected="-1"
    ) -%}
{%- endmacro -%}


{%- macro mat_execute_build_sql(materialization_config, existing_relation, build_sql, post_hooks) -%}

    -- `BEGIN` happens here:
    {{- run_hooks(pre_hooks, inside_transaction=True) -}}

    {%- set grant_config = config.get('grants') -%}

    {%- call statement(name="main") -%}
        {{ build_sql }}
    {%- endcall -%}

    {%- set should_revoke = should_revoke(existing_relation, full_refresh_mode=True) -%}
    {%- do apply_grants(materialization_config.fully_qualified_path, grant_config, should_revoke=should_revoke) -%}

    {{- run_hooks(post_hooks, inside_transaction=True) -}}

    {{- adapter.commit() -}}

{%- endmacro -%}
