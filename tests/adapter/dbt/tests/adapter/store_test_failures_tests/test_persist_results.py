import pytest

from dbt.contracts.results import TestStatus
from dbt.tests.util import run_dbt


seeds__chipmunks_stage = """
name,shirt
alvin,red
simon,blue
theodore,green
""".strip()


models__chipmunks = """
{{ config(materialized='view') }}
select *
from {{ ref('chipmunks_stage') }}
"""

tests__fail_with_view_strategy = """
{{ config(strategy="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


tests__pass_with_view_strategy = """
{{ config(strategy="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'purple'
"""


class TestPersistResults:
    @pytest.fixture(scope="function", autouse=True)
    def setup_teardown(self, project):
        run_dbt(["seed"])
        run_dbt(["run"])

        yield

        with project.adapter.connection_named("__test"):
            test_results_schema = project.adapter.Relation.create(
                database=project.database, schema=f"{project.test_schema}_dbt_test__audit"
            )
            relations_schema = project.adapter.Relation.create(
                database=project.database, schema=project.test_schema
            )
            project.adapter.drop_schema(test_results_schema)
            project.adapter.drop_schema(relations_schema)

    @pytest.fixture(scope="class")
    def seeds(self):
        return {"chipmunks_stage.csv": seeds__chipmunks_stage}

    @pytest.fixture(scope="class")
    def models(self):
        return {"chipmunks.sql": models__chipmunks}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fail_with_view_strategy.sql": tests__fail_with_view_strategy,
            "pass_with_view_strategy.sql": tests__pass_with_view_strategy,
        }

    def test_tests_run_successfully_and_are_persisted_as_views(self, project):
        results = run_dbt(["test"], expect_pass=False)
        actual_results = {(result.node.name, result.status) for result in results}
        expected_results = {
            ("pass_with_view_strategy", TestStatus.Pass),
            ("fail_with_view_strategy", TestStatus.Fail),
        }
        assert actual_results == expected_results
