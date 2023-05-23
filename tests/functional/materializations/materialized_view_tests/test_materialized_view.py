import pytest

from dbt.contracts.results import RunStatus
from tests.adapter.dbt.tests.adapter.materialized_view.base import (
    Base,
    run_model,
    assert_relation_is_materialized_view,
    insert_record,
    get_row_count,
)

from tests.functional.materializations.materialized_view_tests.fixtures import (
    PostgresOnConfigurationChangeBase,
)


class TestBasic(Base):
    def test_relation_is_materialized_view_on_initial_creation(self, project):
        assert_relation_is_materialized_view(project, self.base_materialized_view)
        with pytest.raises(AssertionError):
            assert_relation_is_materialized_view(project, self.base_table)

    def test_relation_is_materialized_view_when_rerun(self, project):
        run_model(self.base_materialized_view.name)
        assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_relation_is_materialized_view_on_full_refresh(self, project):
        run_model(self.base_materialized_view.name, full_refresh=True)
        assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_relation_is_materialized_view_on_update(self, project):
        run_model(
            self.base_materialized_view.name, run_args=["--vars", "quoting: {identifier: True}"]
        )
        assert_relation_is_materialized_view(project, self.base_materialized_view)

    def test_updated_base_table_data_only_shows_in_materialized_view_after_rerun(self, project):
        table_start = get_row_count(project, self.base_table)
        view_start = get_row_count(project, self.base_materialized_view)

        new_record = (2,)
        insert_record(project, new_record, self.base_table)

        table_mid = get_row_count(project, self.base_table)
        view_mid = get_row_count(project, self.base_materialized_view)
        assert view_start == table_start
        assert view_mid == table_start
        assert table_mid > table_start

        run_model(self.base_materialized_view.name)

        table_end = get_row_count(project, self.base_table)
        view_end = get_row_count(project, self.base_materialized_view)
        assert table_end == table_mid
        assert view_end > view_mid
        assert view_end == table_end


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
