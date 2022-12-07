import pytest
from dbt.tests.util import (
    run_dbt,
    get_manifest,
    run_dbt_and_capture
)

# Verify only SQL table materializations work with constraints

# Verify manfiest is updated with the correct constraints, data_types, and checks


# create a sql model fixture
my_model_sql = """
{{
  config(
    materialized = "table"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

my_model_constraints_disabled_sql = """
{{
  config(
    materialized = "table",
    constraints_enabled = false
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

# create a yaml fixture
model_schema_yml = """
version: 2
models:
  - name: my_model
    config:
      constraints_enabled: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
        data_type: date
"""

my_model_error_sql = """
{{
  config(
    materialized = "view"
  )
}}

select
  1 as id,
  'blue' as color,
  cast('2019-01-01' as date) as date_day
"""

model_schema_errors_yml = """
version: 2
models:
  - name: my_model
    config:
      constraints_enabled: true
    columns:
      - name: id
        data_type: integer
        description: hello
        constraints: ['not null','primary key']
        check: (id > 0)
        tests:
          - unique
      - name: color
        data_type: text
      - name: date_day
"""


class BaseConstraintsEnabledModelvsProject:
    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "+constraints_enabled": True,
                    "subdirectory": {
                        "+constraints_enabled": False,
                    },
                }
            }
        }


class TestModelLevelConstraintsEnabledConfigs(BaseConstraintsEnabledModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__model_constraints_enabled_true(self, project):

        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled

        assert constraints_enabled_actual_config is True

        # expected_columns = {}
        # expected_constraints = {}
        # expected_checks = {}


class TestModelLevelConstraintsDisabledConfigs(BaseConstraintsEnabledModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_constraints_disabled_sql,
            "constraints_schema.yml": model_schema_yml,
        }

    def test__model_constraints_enabled_true(self, project):

        run_dbt(["run"])
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled

        assert constraints_enabled_actual_config is False


# If there is no schema config, show a compilation error
class TestSchemaConstraintsEnabledConfigs(BaseConstraintsEnabledModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_sql,
        }

    def test__schema_error(self, project):
        schema_error_expected = "Schema Error: `yml` configuration does NOT exist"
        results, log_output = run_dbt_and_capture(['run'], expect_pass=False)
        assert schema_error_expected in log_output


class TestModelLevelConstraintsErrorMessages(BaseConstraintsEnabledModelvsProject):
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "my_model.sql": my_model_error_sql,
            "constraints_schema.yml": model_schema_errors_yml,
        }

    def test__config_errors(self, project):

        results, log_output = run_dbt_and_capture(['run'], expect_pass=False)
        manifest = get_manifest(project.project_root)
        model_id = "model.test.my_model"
        my_model_config = manifest.nodes[model_id].config
        constraints_enabled_actual_config = my_model_config.constraints_enabled

        assert constraints_enabled_actual_config is True

        expected_materialization_error = "Materialization Error: {'materialization': 'view'}"
        expected_empty_data_type_error = "Columns with `data_type` Blank/Null Errors: {'date_day'}"
        assert expected_materialization_error in log_output
        assert expected_empty_data_type_error in log_output
