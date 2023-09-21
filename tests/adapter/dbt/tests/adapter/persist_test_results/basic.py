from collections import namedtuple
from typing import Dict

import pytest

from dbt.contracts.results import TestStatus
from dbt.tests.util import run_dbt, check_relation_types

from dbt.tests.adapter.persist_test_results._files import (
    SEED__CHIPMUNKS,
    MODEL__CHIPMUNKS,
    TEST__FAIL_WITH_VIEW_STRATEGY,
    TEST__PASS_WITH_VIEW_STRATEGY,
    TEST__FAIL_WITH_TABLE_STRATEGY,
    TEST__PASS_WITH_TABLE_STRATEGY,
)


class PersistTestResults:
    seed_table: str = "chipmunks_stage"
    model_table: str = "chipmunks"

    audit_schema: str

    @pytest.fixture(scope="class", autouse=True)
    def setup_teardown_class(self, project):
        # the seed doesn't get touched, load it once
        run_dbt(["seed"])
        yield

    @pytest.fixture(scope="function", autouse=True)
    def setup_teardown_method(self, project, setup_teardown_class):
        # make sure the model is always right
        run_dbt(["run"])

        # the name of the audit schema doesn't change in a class, but the fixtures run out of order for some reason
        self.audit_schema = f"{project.test_schema}_dbt_test__audit"

        yield

        # clear out the audit schema after each test case
        with project.adapter.connection_named("__test"):
            audit_schema = project.adapter.Relation.create(
                database=project.database, schema=self.audit_schema
            )
            project.adapter.drop_schema(audit_schema)

    @pytest.fixture(scope="class")
    def seeds(self):
        return {f"{self.seed_table}.csv": SEED__CHIPMUNKS}

    @pytest.fixture(scope="class")
    def models(self):
        return {f"{self.model_table}.sql": MODEL__CHIPMUNKS}

    @pytest.fixture(scope="class")
    def tests(self):
        return {
            "fail_with_view_strategy.sql": TEST__FAIL_WITH_VIEW_STRATEGY,
            "pass_with_view_strategy.sql": TEST__PASS_WITH_VIEW_STRATEGY,
            "fail_with_table_strategy.sql": TEST__FAIL_WITH_TABLE_STRATEGY,
            "pass_with_table_strategy.sql": TEST__PASS_WITH_TABLE_STRATEGY,
        }

    def row_count(self, project, relation_name: str) -> int:
        """
        Return the row count for the relation.

        Args:
            project: the project fixture
            relation_name: the name of the relation

        Returns:
            the row count as an integer
        """
        raise NotImplementedError(
            "To use this test, please implement `get_audit_relation_summary`, inherited from `PersistTestResults`."
        )

    def insert_record(self, project, record: Dict[str, str]):
        raise NotImplementedError(
            "To use this test, please implement `insert_record`, inherited from `PersistTestResults`."
        )

    def delete_record(self, project, record: Dict[str, str]):
        raise NotImplementedError(
            "To use this test, please implement `delete_record`, inherited from `PersistTestResults`."
        )

    def test_tests_run_successfully_and_are_persisted_correctly(self, project):
        # set up the expected results
        TestResult = namedtuple("TestResult", ["name", "status", "type", "row_count"])
        expected_results = {
            TestResult("pass_with_view_strategy", TestStatus.Pass, "view", 0),
            TestResult("fail_with_view_strategy", TestStatus.Fail, "view", 1),
            TestResult("pass_with_table_strategy", TestStatus.Pass, "table", 0),
            TestResult("fail_with_table_strategy", TestStatus.Fail, "table", 1),
        }

        # run the tests once
        results = run_dbt(["test"], expect_pass=False)

        # show that the statuses are what we expect
        actual = {(result.node.name, result.status) for result in results}
        expected = {(result.name, result.status) for result in expected_results}
        assert actual == expected

        # show that the results are persisted in the correct database objects
        check_relation_types(
            project.adapter, {result.name: result.type for result in expected_results}
        )

        # show that only the failed records show up
        actual = {
            (result.name, self.row_count(project, result.name)) for result in expected_results
        }
        expected = {(result.name, result.row_count) for result in expected_results}
        assert actual == expected

        # insert a new record in the model that fails the "pass" tests
        # show that the view updates, but not the table
        self.insert_record(project, {"name": "dave", "shirt": "purple"})
        expected_results.remove(TestResult("pass_with_view_strategy", TestStatus.Pass, "view", 0))
        expected_results.add(TestResult("pass_with_view_strategy", TestStatus.Pass, "view", 1))

        # delete the original record from the model that failed the "fail" tests
        # show that the view updates, but not the table
        self.delete_record(project, {"name": "theodore", "shirt": "green"})
        expected_results.remove(TestResult("fail_with_view_strategy", TestStatus.Fail, "view", 1))
        expected_results.add(TestResult("fail_with_view_strategy", TestStatus.Fail, "view", 0))

        # show that the views update without needing to run dbt, but the tables do not update
        actual = {
            (result.name, self.row_count(project, result.name)) for result in expected_results
        }
        expected = {(result.name, result.row_count) for result in expected_results}
        assert actual == expected
