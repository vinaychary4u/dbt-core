import pytest
import re

from dbt.tests.util import run_dbt, get_manifest, run_dbt_and_capture, write_file

from dbt.tests.adapter.constraints.fixtures import (
    my_model_sql,
    my_model_wrong_order_sql,
    my_model_wrong_name_sql,
    model_schema_yml,
)


class BaseConstraintsColumnsEqual:
    """
    dbt should catch these mismatches during its "preflight" checks.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model_wrong_order.sql": my_model_wrong_order_sql,
            "my_model_wrong_name.sql": my_model_wrong_name_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__constraints_wrong_column_order(self, project):

        results, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_order"], expect_pass=False
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_order"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled

        assert constraints_enabled_actual_config is True

        expected_compile_error = "Please ensure the name, order, and number of columns in your `yml` file match the columns in your SQL file."
        expected_schema_file_columns = "Schema File Columns: ['ID', 'COLOR', 'DATE_DAY']"
        expected_sql_file_columns = "SQL File Columns: ['COLOR', 'ID', 'DATE_DAY']"

        assert expected_compile_error in log_output
        assert expected_schema_file_columns in log_output
        assert expected_sql_file_columns in log_output

    def test__constraints_wrong_column_names(self, project):
        results, log_output = run_dbt_and_capture(
            ["run", "-s", "my_model_wrong_name"], expect_pass=False
        )
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model_wrong_name"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled

        assert constraints_enabled_actual_config is True

        expected_compile_error = "Please ensure the name, order, and number of columns in your `yml` file match the columns in your SQL file."
        expected_schema_file_columns = "Schema File Columns: ['ID', 'COLOR', 'DATE_DAY']"
        expected_sql_file_columns = "SQL File Columns: ['ERROR', 'COLOR', 'DATE_DAY']"

        assert expected_compile_error in log_output
        assert expected_schema_file_columns in log_output
        assert expected_sql_file_columns in log_output


# This is SUPER specific to Postgres, and will need replacing on other adapters
# TODO: make more generic
_expected_sql = """
create table "{0}"."{1}"."my_model__dbt_tmp" (
    id integer not null primary key check (id > 0) ,
    color text ,
    date_day date
) ;
insert into "{0}"."{1}"."my_model__dbt_tmp" (
    id ,
    color ,
    date_day
) (
    select 1 as id,
    'blue' as color,
    cast('2019-01-01' as date) as date_day
);
"""


class BaseConstraintsRuntimeEnforcement:
    """
    These constraints pass muster for dbt's preflight checks. Make sure they're
    passed into the DDL statement. If they don't match up with the underlying data,
    the data platform should raise an error at runtime.
    """

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    @pytest.fixture(scope="class")
    def expected_sql(self, project):
        return _expected_sql.format(project.database, project.test_schema)

    def test__constraints_ddl(self, project, expected_sql):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1
        # TODO: consider refactoring this to introspect logs instead
        with open("./target/run/test/models/my_model.sql", "r") as fp:
            generated_sql = fp.read()

        generated_sql_check = re.sub(r"\s+", " ", generated_sql).lower().strip()
        expected_sql_check = re.sub(r"\s+", " ", expected_sql).lower().strip()
        assert (
            expected_sql_check == generated_sql_check
        ), f"generated sql did not match expected: \n{generated_sql} \n{expected_sql}"

    def test__constraints_enforcement_rollback(self, project, unique_schema):
        results = run_dbt(["run", "-s", "my_model"])
        assert len(results) == 1

        # Make a contract-breaking change to the model
        my_model_with_nulls = my_model_sql.replace("1 as id", "null as id")
        write_file(my_model_with_nulls, "models", "my_model.sql")

        failing_results = run_dbt(["run", "-s", "my_model"], expect_pass=False)
        assert len(failing_results) == 1

        # Verify the previous table still exists
        old_model_exists_sql = """
            select id from {0}.{1}.my_model where id = 1
        """.format(
            project.database, project.test_schema
        )
        old_model_exists = project.run_sql(old_model_exists_sql, fetch="all")
        assert len(old_model_exists) == 1
        assert old_model_exists[0][0] == 1

        # Confirm this model was contracted
        # TODO: is this step really necessary?
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled
        assert constraints_enabled_actual_config is True

        # Its result includes this error message
        expected_constraints_error = 'null value in column "id"'
        expected_violation_error = "violates not-null constraint"
        assert expected_constraints_error in failing_results[0].message
        assert expected_violation_error in failing_results[0].message


class TestConstraintsColumnsEqual(BaseConstraintsColumnsEqual):
    pass


class TestConstraintsRuntimeEnforcement(BaseConstraintsRuntimeEnforcement):
    pass
