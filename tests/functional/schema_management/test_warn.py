import pytest
import os

from dbt.tests.util import (
    run_dbt,
    check_table_does_exist,
)

model = """
{{
  config(
    materialized = "table"
  )
}}
SELECT * FROM (
VALUES (1, 'one'),
 (2, 'two'),
 (3, 'three')
) AS t (num,letter)
"""


class TestDanglingModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model,
            "model_b.sql": model,
        }

    @pytest.fixture(scope="class")
    def dbt_profile_target(self):
        return {
            "type": "postgres",
            "threads": 4,
            "host": "localhost",
            "port": int(os.getenv("POSTGRES_TEST_PORT", 5432)),
            "user": os.getenv("POSTGRES_TEST_USER", "root"),
            "pass": os.getenv("POSTGRES_TEST_PASS", "password"),
            "dbname": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
            "manage_schemas": True,
        }

    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": unique_schema,
                    "action": "warn",
                }
            ]
        }

    def test_drop(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model,
            }
        )
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")
