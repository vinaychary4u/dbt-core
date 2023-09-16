from typing import Dict, Set, Tuple

import pytest

from dbt.contracts.results import TestStatus
from dbt.tests.util import run_dbt

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

    def get_audit_relation_summary(self, project) -> Set[Tuple]:
        """
        Return summary stats about each relation in the audit schema to be verified in a test.

        Args:
            project: the project fixture

        Returns:
            Relation stats, e.g.:
            {
                ("my_table", "table", 0)
                ("my_view", "view", 1)
            }
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
        # run the tests once
        results = run_dbt(["test"], expect_pass=False)

        # make sure the results are what we expect
        actual_results = {(result.node.name, result.status) for result in results}
        expected_results = {
            ("pass_with_view_strategy", TestStatus.Pass),
            ("fail_with_view_strategy", TestStatus.Fail),
            ("pass_with_table_strategy", TestStatus.Pass),
            ("fail_with_table_strategy", TestStatus.Fail),
        }
        assert actual_results == expected_results

        # show that the results are persisted in the correct database objects
        persisted_objects = self.get_audit_relation_summary(project)
        assert persisted_objects == {
            ("pass_with_view_strategy", "view", 0),
            ("fail_with_view_strategy", "view", 1),
            ("pass_with_table_strategy", "table", 0),
            ("fail_with_table_strategy", "table", 1),
        }

        # insert a new record in the model that fails the "pass" tests
        self.insert_record(project, {"name": "dave", "shirt": "purple"})

        # delete the original record from the model that failed the "fail" tests
        self.delete_record(project, {"name": "theodore", "shirt": "green"})

        # show that the views update and the tables do not
        persisted_objects = self.get_audit_relation_summary(project)
        assert persisted_objects == {
            ("pass_with_view_strategy", "view", 1),  # the views update
            ("fail_with_view_strategy", "view", 0),  # the views update
            ("pass_with_table_strategy", "table", 0),  # the tables do not update
            ("fail_with_table_strategy", "table", 1),  # the tables do not update
        }
