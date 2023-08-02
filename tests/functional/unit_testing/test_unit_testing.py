import pytest
from dbt.tests.util import run_dbt

my_model_sql = """
SELECT
a+b as c,
concat(string_a, string_b) as string_c,
not_testing, date_a,
{{ dbt.string_literal(type_numeric()) }} as macro_call,
{{ dbt.string_literal(var('my_test')) }} as var_call,
{{ dbt.string_literal(env_var('MY_TEST', 'default')) }} as env_var_call,
{{ dbt.string_literal(invocation_id) }} as invocation_id
FROM {{ ref('my_model_a')}} my_model_a
JOIN {{ ref('my_model_b' )}} my_model_b
ON my_model_a.id = my_model_b.id
"""

my_model_a_sql = """
SELECT
1 as a,
1 as id,
2 as not_testing,
'a' as string_a,
DATE '2020-01-02' as date_a
"""

my_model_b_sql = """
SELECT
2 as b,
1 as id,
2 as c,
'b' as string_b
"""

test_my_model_yml = """
unit:
  - model: my_model
    tests:
      - name: test_my_model
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, a: 1}
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        expect:
          - {c: 2}

      - name: test_my_model_empty
        given:
          - input: ref('my_model_a')
            rows: []
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        expect: []
      - name: test_my_model_overrides
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, a: 1}
          - input: ref('my_model_b')
            rows:
              - {id: 1, b: 2}
              - {id: 2, b: 2}
        overrides:
          macros:
            type_numeric: override
            invocation_id: 123
          vars:
            my_test: var_override
          env_vars:
            MY_TEST: env_var_override
        expect:
          - {macro_call: override, var_call: var_override, env_var_call: env_var_override, invocation_id: 123}
      - name: test_my_model_string_concat
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, string_a: a}
          - input: ref('my_model_b')
            rows:
              - {id: 1, string_b: b}
        expect:
          - {string_c: ab}
      - name: test_my_model_datetime
        given:
          - input: ref('my_model_a')
            rows:
              - {id: 1, date_a: "2020-01-01"}
          - input: ref('my_model_b')
            rows:
              - {id: 1}
        expect:
          - {date_a: "2020-01-01"}
"""

packages_yml = """
packages:
  - git: "https://github.com/dbt-labs/dbt-utils.git"
    revision: 0.9.2
"""


class TestUnitTests:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "my_model_a.sql": my_model_a_sql,
            "my_model_b.sql": my_model_b_sql,
            "test_my_model.yml": test_my_model_yml,
        }

    @pytest.fixture(scope="class")
    def packages(self):
        return packages_yml

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {"vars": {"my_test": "my_test_var"}}

    def test_basic(self, project):
        run_dbt(["deps"])
        results = run_dbt(["run"])
        assert len(results) == 3

        results = run_dbt(["unit-test", "--select", "my_model"], expect_pass=False)
        assert len(results) == 5
