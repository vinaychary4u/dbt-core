{# ------- BOOLEAN MACROS --------- #}




{#
  -- SUPPORT MULTIPLE GRANTEES PER DCL STATEMENT
  -- Does this database support 'grant {privilege} to {grantee_1}, {grantee_2}, ...'
  -- Or must these be separate statements:
  --  `grant {privilege} to {grantee_1}`;
  --  `grant {privilege} to {grantee_2}`;
  -- By default, pick the former, because it's what we prefer when available.
#}

{% macro support_multiple_grantees_per_dcl_statement() %}
    {{ return(adapter.dispatch('support_multiple_grantees_per_dcl_statement', 'dbt')()) }}
{% endmacro %}

{%- macro default__support_multiple_grantees_per_dcl_statement() -%}
    {{ return(True) }}
{%- endmacro -%}



{# ------- RUNTIME APPLICATION --------- #}

{% macro get_dcl_statement_list(relation, grant_config, get_dcl_macro) %}
    {{ return(adapter.dispatch('get_dcl_statement_list', 'dbt')(relation, grant_config, get_dcl_macro)) }}
{% endmacro %}

{%- macro default__get_dcl_statement_list(relation, grant_config, get_dcl_macro) -%}
    {#
      -- Unpack grant_config into specific privileges and the set of users who need them granted/revoked.
      -- Depending on whether this database supports multiple grantees per statement, pass in the list of
      -- all grantees per privilege, or (if not) template one statement per privilege-grantee pair.
      -- `get_dcl_macro` will be either `get_grant_sql` or `get_revoke_sql`
    #}
    {%- set dcl_statements = [] -%}
    {%- for privilege, grantees in grant_config.items() %}
        {%- if support_multiple_grantees_per_dcl_statement() and grantees -%}
          {%- set dcl = get_dcl_macro(relation, privilege, grantees) -%}
          {%- do dcl_statements.append(dcl) -%}
        {%- else -%}
          {%- for grantee in grantees -%}
              {% set dcl = get_dcl_macro(relation, privilege, [grantee]) %}
              {%- do dcl_statements.append(dcl) -%}
          {% endfor -%}
        {%- endif -%}
    {%- endfor -%}
    {{ return(dcl_statements) }}
{%- endmacro %}


{% macro call_dcl_statements(dcl_statement_list) %}
    {{ return(adapter.dispatch("call_dcl_statements", "dbt")(dcl_statement_list)) }}
{% endmacro %}

{% macro default__call_dcl_statements(dcl_statement_list) %}
    {#
      -- By default, supply all grant + revoke statements in a single semicolon-separated block,
      -- so that they're all processed together.

      -- Some databases do not support this. Those adapters will need to override this macro
      -- to run each statement individually.
    #}
    {% call statement('grants') %}
        {% for dcl_statement in dcl_statement_list %}
            {{ dcl_statement }};
        {% endfor %}
    {% endcall %}
{% endmacro %}


{% macro apply_grants(relation, grant_config, should_revoke) %}
    {{ return(adapter.dispatch("apply_grants", "dbt")(relation, grant_config, should_revoke)) }}
{% endmacro %}

{% macro default__apply_grants(relation, grant_config, should_revoke=True) %}
    {#-- If grant_config is {} or None, this is a no-op --#}
    {% if grant_config %}
        {% if should_revoke %}
            {#-- We think previous grants may have carried over --#}
            {#-- Show current grants and calculate diffs --#}
            {% set current_grants_table = run_query(get_show_grant_sql(relation)) %}
            {% set current_grants_dict = adapter.standardize_grants_dict(current_grants_table) %}
            {% set needs_granting = diff_of_two_dicts(grant_config, current_grants_dict) %}
            {% set needs_revoking = diff_of_two_dicts(current_grants_dict, grant_config) %}
            {% if not (needs_granting or needs_revoking) %}
                {{ log('On ' ~ relation ~': All grants are in place, no revocation or granting needed.')}}
            {% endif %}
        {% else %}
            {#-- We don't think there's any chance of previous grants having carried over. --#}
            {#-- Jump straight to granting what the user has configured. --#}
            {% set needs_revoking = {} %}
            {% set needs_granting = grant_config %}
        {% endif %}
        {% if needs_granting or needs_revoking %}
            {% set revoke_statement_list = get_dcl_statement_list(relation, needs_revoking, get_revoke_sql) %}
            {% set grant_statement_list = get_dcl_statement_list(relation, needs_granting, get_grant_sql) %}
            {% set dcl_statement_list = revoke_statement_list + grant_statement_list %}
            {% if dcl_statement_list %}
                {{ call_dcl_statements(dcl_statement_list) }}
            {% endif %}
        {% endif %}
    {% endif %}
{% endmacro %}
