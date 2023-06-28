{% materialization materialized_view, default %}

    -- Try to create a valid materialized view from the config before doing anything else
    {% set new_materialized_view = adapter.Relation.from_model_node(config.model) %}

    -- We still need these because they tie into the existing process (e.g. RelationBase vs. RelationConfigBase)
    {% set existing_relation = load_cached_relation_config(new_materialized_view) %}
    {% set target_relation = this.incorporate(type=this.MaterializedView) %}

    {{ mat_setup(new_materialized_view, pre_hooks) }}

        {% set build_sql = materialized_view_build_sql(new_materialized_view, existing_relation) %}
        {% set relation_exists = existing_relation is not none %}

        {% if build_sql == '' %}
            {{ mat_execute_no_op(new_materialized_view) }}
        {% else %}
            {{ mat_execute_build_sql(new_materialized_view, build_sql, relation_exists, post_hooks) }}
        {% endif %}

    {{ mat_teardown(new_materialized_view, post_hooks) }}

    {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}


{% macro materialized_view_build_sql(new_materialized_view, existing_relation) %}

    -- determine the scenario we're in: create, full_refresh, alter
    {% if existing_relation is none %}
        {% set build_sql = create_materialized_view_sql(new_materialized_view) %}
    {% elif should_full_refresh() or not existing_relation.is_materialized_view %}
        {% set build_sql = replace_materialized_view_sql(new_materialized_view, existing_relation) %}
    {% else %}
        {% set build_sql = alter_materialized_view_with_on_configuration_option_sql(new_materialized_view) %}
    {% endif %}

    {% do return(build_sql) %}

{% endmacro %}


{% macro alter_materialized_view_with_on_configuration_option_sql(new_materialized_view) %}

    {% set describe_relation_results = describe_materialized_view_sql(new_materialized_view) %}
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
