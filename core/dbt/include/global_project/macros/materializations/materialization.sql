{%- macro execute_no_op(materialization) -%}
    {%- do store_raw_result(
        name='main',
        message='skip ' ~ materialization,
        code='skip',
        rows_affected='-1'
    ) -%}
{%- endmacro -%}


{%- macro execute_build_sql(materialization, build_sql, pre_hooks, post_hooks) -%}

    -- `BEGIN` happens here:
    {{- run_hooks(pre_hooks, inside_transaction=True) -}}

    {%- call statement(name='main') -%}
        {{ build_sql }}
    {%- endcall -%}

    {%- do apply_grants(materialization, materialization.grant_config, materialization.should_revoke_grants) -%}

    {{- run_hooks(post_hooks, inside_transaction=True) -}}

    {{- adapter.commit() -}}

{%- endmacro -%}
