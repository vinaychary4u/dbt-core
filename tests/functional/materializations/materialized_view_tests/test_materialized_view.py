from dbt.contracts.results import RunStatus
from tests.adapter.dbt.tests.adapter.materialized_view.base import Base, run_model

from tests.functional.materializations.materialized_view_tests.fixtures import (
    PostgresOnConfigurationChangeBase,
)


class TestBasic(Base):
    def test_relation_is_materialized_view_on_initial_creation(self, project):
        self.assert_relation_is_materialized_view(project)

    def test_relation_is_materialized_view_when_rerun(self, project):
        run_model(self.base_materialized_view.name)
        self.assert_relation_is_materialized_view(project)

    def test_relation_is_materialized_view_on_full_refresh(self, project):
        run_model(self.base_materialized_view.name, full_refresh=True)
        self.assert_relation_is_materialized_view(project)

    def test_relation_is_materialized_view_on_update(self, project):
        run_model(
            self.base_materialized_view.name, run_args=["--vars", "quoting: {identifier: True}"]
        )
        self.assert_relation_is_materialized_view(project)

    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        self.insert_records(project, self.inserted_records)
        assert self.get_records(project) == self.starting_records

        run_model(self.base_materialized_view.name)
        assert self.get_records(project) == self.starting_records + self.inserted_records


class OnConfigurationChangeCommon(PostgresOnConfigurationChangeBase):
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


class TestOnConfigurationChangeApply(OnConfigurationChangeCommon):

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


class TestOnConfigurationChangeSkip(OnConfigurationChangeCommon):

    on_configuration_change = "skip"

    def test_model_is_skipped_with_configuration_changes(
        self, configuration_changes, configuration_change_skip_message
    ):
        results, logs = run_model(self.base_materialized_view.name)
        self.assert_proper_scenario(
            results, logs, RunStatus.Success, messages_in_logs=[configuration_change_skip_message]
        )


class TestOnConfigurationChangeFail(OnConfigurationChangeCommon):

    on_configuration_change = "fail"

    def test_run_fails_with_configuration_changes(
        self, configuration_changes, configuration_change_fail_message
    ):
        results, logs = run_model(self.base_materialized_view.name, expect_pass=False)
        self.assert_proper_scenario(
            results, logs, RunStatus.Error, messages_in_logs=[configuration_change_fail_message]
        )
