from copy import deepcopy

import pytest

from dbt.contracts.results import RunStatus
from dbt.tests.util import relation_from_name
from tests.adapter.dbt.tests.adapter.materialized_view.base import Model, run_model
from tests.adapter.dbt.tests.adapter.materialized_view.on_configuration_change import (
    OnConfigurationChangeBase,
    get_project_config,
    set_project_config,
)


_BASE_TABLE = """
{{ config(
    materialized='table',
    indexes=[{'columns': ['id'], 'type': 'hash'}]
) }}
select
    1 as id,
    100 as value
where 0 = 1
"""


class PostgresOnConfigurationChangeBase(OnConfigurationChangeBase):

    base_table = Model(name="base_table", definition=_BASE_TABLE, columns=["id", "value"])

    starting_records = [(1, 100)]
    inserted_records = [(2, 200)]

    @pytest.fixture(scope="function")
    def configuration_changes(self, project):
        initial_config = get_project_config(project)

        # change the index from `id` to `value`
        new_config = deepcopy(initial_config)
        new_config["models"].update({"indexes": [{"columns": ["value"], "type": "hash"}]})
        set_project_config(project, new_config)

        yield

        # set this back for the next test
        set_project_config(project, initial_config)

    @pytest.fixture(scope="function")
    def update_index_message(self, project):
        return f"Applying UPDATE INDEXES to: {relation_from_name(project.adapter, self.base_materialized_view.name)}"

    def test_full_refresh_takes_precedence_over_any_configuration_changes(
        self, configuration_changes, replace_message, configuration_change_message
    ):
        results, logs = run_model(self.base_materialized_view.name, full_refresh=True)
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[replace_message],
            messages_not_in_logs=[configuration_change_message],
        )

    def test_model_is_refreshed_with_no_configuration_changes(
        self, refresh_message, configuration_change_message
    ):
        results, logs = run_model(self.base_materialized_view.name)
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[refresh_message, configuration_change_message],
        )


class TestOnConfigurationChangeApply(PostgresOnConfigurationChangeBase):

    on_configuration_change = "apply"

    def test_model_applies_changes_with_configuration_changes(
        self, configuration_changes, alter_message, update_index_message
    ):
        results, logs = run_model(self.base_materialized_view.name)
        self.assert_proper_scenario(
            results,
            logs,
            RunStatus.Success,
            messages_in_logs=[alter_message, update_index_message],
        )


class TestOnConfigurationChangeSkip(PostgresOnConfigurationChangeBase):

    on_configuration_change = "skip"

    def test_model_is_skipped_with_configuration_changes(
        self, configuration_changes, configuration_change_skip_message
    ):
        results, logs = run_model(self.base_materialized_view.name)
        self.assert_proper_scenario(
            results, logs, RunStatus.Success, messages_in_logs=[configuration_change_skip_message]
        )


class TestOnConfigurationChangeFail(PostgresOnConfigurationChangeBase):

    on_configuration_change = "fail"

    def test_run_fails_with_configuration_changes(
        self, configuration_changes, configuration_change_fail_message
    ):
        results, logs = run_model(self.base_materialized_view.name, expect_pass=False)
        self.assert_proper_scenario(
            results, logs, RunStatus.Error, messages_in_logs=[configuration_change_fail_message]
        )
