{% materialization materialized_view, default %}

    -- Try to create a valid materialized view from the config before doing anything else
    {% set new_materialized_view = adapter.Relation.from_runtime_config(config) %}

    -- We still need these because they tie into the existing process (e.g. RelationBase vs. RelationConfigBase)
    {% set existing_relation = load_cached_relation(this) %}
    {% set target_relation = this.incorporate(type=this.MaterializedView) %}
    {% set intermediate_relation = make_intermediate_relation(target_relation) %}
    {% set backup_relation_type = target_relation.MaterializedView if existing_relation is none else existing_relation.type %}
    {% set backup_relation = make_backup_relation(target_relation, backup_relation_type) %}

    {{ materialized_view_setup(backup_relation, intermediate_relation, pre_hooks) }}

        {% set build_sql = materialized_view_build_sql(new_materialized_view, existing_relation, backup_relation, intermediate_relation) %}

        {% if build_sql == '' %}
            {{ materialized_view_execute_no_op(new_materialized_view) }}
        {% else %}
            {{ materialized_view_execute_build_sql(build_sql, new_materialized_view, post_hooks) }}
        {% endif %}

    {{ materialized_view_teardown(backup_relation, intermediate_relation, post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro materialized_view_setup(backup_relation, intermediate_relation, pre_hooks) %}

    -- backup_relation and intermediate_relation should not already exist in the database
    -- it's possible these exist because of a previous run that exited unexpectedly
    {% set preexisting_backup_relation = load_cached_relation(backup_relation) %}
    {% set preexisting_intermediate_relation = load_cached_relation(intermediate_relation) %}

    -- drop the temp relations if they exist already in the database
    {{ drop_relation_if_exists(preexisting_backup_relation) }}
    {{ drop_relation_if_exists(preexisting_intermediate_relation) }}

    {{ run_hooks(pre_hooks, inside_transaction=False) }}

{% endmacro %}


{% macro materialized_view_teardown(backup_relation, intermediate_relation, post_hooks) %}

    -- drop the temp relations if they exist to leave the database clean for the next run
    {{ drop_relation_if_exists(backup_relation) }}
    {{ drop_relation_if_exists(intermediate_relation) }}

    {{ run_hooks(post_hooks, inside_transaction=False) }}

{% endmacro %}


{% macro materialized_view_build_sql(new_materialized_view, existing_relation, backup_relation, intermediate_relation) %}

    {% set full_refresh_mode = should_full_refresh() %}

    -- determine the scenario we're in: create, full_refresh, alter
    {% if existing_relation is none %}
        {% set build_sql = create_materialized_view_sql(new_materialized_view) %}
    {% elif full_refresh_mode or not existing_relation.is_materialized_view %}
        {% set build_sql = replace_materialized_view_sql(new_materialized_view, existing_relation, backup_relation, intermediate_relation) %}
    {% else %}
        {% set build_sql = alter_materialized_view_with_on_configuration_option_sql(new_materialized_view) %}
    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro alter_materialized_view_with_on_configuration_option_sql(new_materialized_view) %}

    {% set describe_relation_results = describe_materialized_view(new_materialized_view) %}
    {% set existing_materialized_view = adapter.Relation.from_describe_relation_results(describe_relation_results, adapter.Relation.MaterializedView) %}
    {% set on_configuration_change = config.get('on_configuration_change') %}

    {% if new_materialized_view == existing_materialized_view %}
        {% set build_sql = refresh_materialized_view_sql(new_materialized_view) %}

    {% elif on_configuration_change == 'apply' %}
        {% set build_sql = alter_materialized_view_sql(new_materialized_view, existing_materialized_view) %}
    {% elif on_configuration_change == 'continue' %}
        {% set build_sql = '' %}
        {{ exceptions.warn("Configuration changes were identified and `on_configuration_change` was set to `continue` for `" ~ new_materialized_view.fully_qualified_path ~ "`") }}
    {% elif on_configuration_change == 'fail' %}
        {{ exceptions.raise_fail_fast_error("Configuration changes were identified and `on_configuration_change` was set to `fail` for `" ~ new_materialized_view.fully_qualified_path ~ "`") }}

    {% else %}
        -- this only happens if the user provides a value other than `apply`, 'continue', 'fail', which should have already raised an exception
        {{ exceptions.raise_compiler_error("Unexpected configuration scenario: `" ~ on_configuration_change ~ "`") }}

    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro materialized_view_execute_no_op(new_materialized_view) %}
    {% do store_raw_result(
        name="main",
        message="skip " ~ new_materialized_view.fully_qualified_path,
        code="skip",
        rows_affected="-1"
    ) %}
{% endmacro %}


{% macro materialized_view_execute_build_sql(build_sql, new_materialized_view, post_hooks) %}

    -- `BEGIN` happens here:
    {{ run_hooks(pre_hooks, inside_transaction=True) }}

    {% set grant_config = config.get('grants') %}

    {% call statement(name="main") %}
        {{ build_sql }}
    {% endcall %}

    {% set should_revoke = should_revoke(new_materialized_view.fully_qualified_path, full_refresh_mode=True) %}
    {% do apply_grants(new_materialized_view.fully_qualified_path, grant_config, should_revoke=should_revoke) %}

    {% do persist_docs(new_materialized_view.fully_qualified_path, model) %}

    {{ run_hooks(post_hooks, inside_transaction=True) }}

    {{ adapter.commit() }}

{% endmacro %}
