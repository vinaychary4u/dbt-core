{#
  Postgres tables have a maximum length of 63 characters, anything longer is silently truncated.
  Temp and backup relations add a lot of extra characters to the end of table names to ensure uniqueness.
  To prevent this going over the character limit, the base_relation name is truncated to ensure
  that name + suffix + uniquestring is < 63 characters.
#}


{% macro postgres__make_relation_with_suffix(base_relation, suffix, dstring) %}
    {% if dstring %}
      {% set dt = modules.datetime.datetime.now() %}
      {% set dtstring = dt.strftime("%H%M%S%f") %}
      {% set suffix = suffix ~ dtstring %}
    {% endif %}
    {% set suffix_length = suffix|length %}
    {% set relation_max_name_length = base_relation.relation_max_name_length() %}
    {% if suffix_length > relation_max_name_length %}
        {% do exceptions.raise_compiler_error('Relation suffix is too long (' ~ suffix_length ~ ' characters). Maximum length is ' ~ relation_max_name_length ~ ' characters.') %}
    {% endif %}
    {% set identifier = base_relation.identifier[:relation_max_name_length - suffix_length] ~ suffix %}

    {{ return(base_relation.incorporate(path={"identifier": identifier })) }}

  {% endmacro %}


{% macro postgres__make_intermediate_relation(base_relation, suffix) %}
    {{ return(postgres__make_relation_with_suffix(base_relation, suffix, dstring=False)) }}
{% endmacro %}


{% macro postgres__make_temp_relation(base_relation, suffix) %}
    {% set temp_relation = postgres__make_relation_with_suffix(base_relation, suffix, dstring=True) %}
    {{ return(temp_relation.incorporate(path={"schema": none,
                                              "database": none})) }}
{% endmacro %}


{% macro postgres__make_backup_relation(base_relation, backup_relation_type, suffix) %}
    {% set backup_relation = postgres__make_relation_with_suffix(base_relation, suffix, dstring=False) %}
    {{ return(backup_relation.incorporate(type=backup_relation_type)) }}
{% endmacro %}
