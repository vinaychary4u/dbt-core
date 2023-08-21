import pytest

from dbt.tests.util import run_dbt

parent_materialization = """
{% materialization parent, default %}
    {%- set target_relation = this.incorporate(type='table') %}
    {% call statement('main') -%}
        set session time zone 'Asia/Kolkata';
    {%- endcall %}
    {{ return({'relations': [target_relation]}) }}
{% endmaterialization %}
"""

child_materialization = """
{% materialization child, default %}
    {%- set relations = adapter.dispatch('parent')() %}
    {{ return({'relations': relations['relations'] }) }}
{% endmaterialization %}
"""

my_model_sql = """
{{ config(materialized='child') }}
select current_setting('timezone') as current_tz
"""


class TestMaterializationOverride:
    @pytest.fixture(scope="class")
    def macros(self):
        return {
            "parent.sql": parent_materialization,
            "child.sql": child_materialization,
        }

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model.sql": my_model_sql,
        }

    def test_foo(self, project):
        res = run_dbt(["run"])
        print(res)
