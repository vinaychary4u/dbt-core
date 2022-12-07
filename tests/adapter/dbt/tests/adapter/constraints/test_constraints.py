import pytest
from dbt.tests.util import (
    run_dbt,
    get_manifest,
)

# Verify compilation errors show expected dynamic messages

# Verify config hierarchy overrides work as expected(ex: constraints enabled at the model level override constraints enabled at the project_yml level with True or False values)

# Verify only SQL table materializations work with constraints

# Verify all or nothing data_type configs are enforced

# Verify manfiest is updated with the correct constraints, data_types, and checks


# Verify DDL matches an expectation

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

# expected DDL
my_model_expected_ddl = """
create  table "dbt"."test16703921190279384893_test_constraints"."my_model__dbt_tmp"



  (









      id integer  not null  primary key  check (id > 0) ,









      color string   ,









      date_day date



  )

 ;


    insert into "dbt"."test16703921190279384893_test_constraints"."my_model__dbt_tmp"

  (





      id ,





      color ,





      date_day



  )


     (




select

  1 as id,

  'blue' as color,

  cast('2019-01-01' as date) as date_day

    );
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

# Verify the DDL matches the expectation with an assert statement

# If there is no schema config, show a compilation error
