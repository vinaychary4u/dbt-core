{%- materialization materialized_view, default -%}

    {%- set materialization = adapter.make_materialization_from_runtime_config(config, 'materialized_view') -%}

    {%- set build_sql = materialized_view_build_sql(materialization) -%}

    {{- run_hooks(pre_hooks, inside_transaction=False) -}}

    {%- if build_sql == '' -%}
        {{- execute_no_op(materialization) -}}
    {%- else -%}
        {{- execute_build_sql(materialization, build_sql, pre_hooks, post_hooks) -}}
    {%- endif -%}

    {{- run_hooks(post_hooks, inside_transaction=False) -}}

    {%- set new_base_relation = adapter.base_relation_from_relation_model(materialization.target_relation) -%}
    {{- return({'relations': [new_base_relation]}) -}}

{%- endmaterialization -%}


{%- macro materialized_view_build_sql(materialization) -%}

    {%- if materialization.build_strategy == 'no_op' -%}
        {%- set build_sql = '' -%}

    {%- elif materialization.build_strategy == 'create' -%}
        {%- set build_sql = create_template(materialization.target_relation) -%}

    {%- elif materialization.build_strategy == 'replace' -%}
        {%- set build_sql = replace_template(
            materialization.existing_relation_stub, materialization.target_relation
        ) -%}

    {%- elif materialization.build_strategy == 'alter' -%}

        {% set describe_relation_results = describe_template(materialization.existing_relation_stub ) %}
        {% set existing_relation = materialization.existing_relation(describe_relation_results) %}

        {%- if materialization.on_configuration_change == 'apply' -%}
            {%- set build_sql = alter_template(existing_relation, materialization.target_relation) -%}

        {%- elif materialization.on_configuration_change == 'continue' -%}
            {%- set build_sql = '' -%}
            {{- exceptions.warn(
                "Configuration changes were identified and `on_configuration_change` "
                "was set to `continue` for `" ~ materialization.target_relation ~ "`"
            ) -}}

        {%- elif materialization.on_configuration_change == 'fail' -%}
            {%- set build_sql = '' -%}
            {{- exceptions.raise_fail_fast_error(
                "Configuration changes were identified and `on_configuration_change` "
                "was set to `fail` for `" ~ materialization.target_relation ~ "`"
            ) -}}

        {%- endif -%}

    {%- else -%}

        {{- exceptions.raise_compiler_error("This build strategy is not supported for materialized views: " ~ materialization.build_strategy) -}}

    {%- endif -%}

    {%- do return(build_sql) -%}

{% endmacro %}
