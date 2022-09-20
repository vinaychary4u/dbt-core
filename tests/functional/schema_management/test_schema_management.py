import pytest
import os
from dbt.exceptions import CompilationException
from dbt.tests.util import run_dbt, check_table_does_exist, check_table_does_not_exist


def model(materialized):
    return f"""
    {{{{
      config(
        materialized = "{materialized}"
      )
    }}}}
    SELECT * FROM (
    VALUES (1, 'one'),
     (2, 'two'),
     (3, 'three')
    ) AS t (num,letter)
    """


class Base:
    materialized = "table"

    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_a.sql": model(self.materialized),
            "model_b.sql": model(self.materialized),
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
        }


class TestUnmanagedSchema(Base):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": "some_other_schema",
                    "action": "drop",
                }
            ]
        }

    def test_should_raise_exception(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        with pytest.raises(CompilationException):
            run_dbt(["--warn-error", "manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

    def test_should_not_delete_anything(
        self,
        project,
    ):
        run_dbt(["run"])
        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")

        project.update_models(
            {
                "model_b.sql": model(self.materialized),
            }
        )
        run_dbt(["manage"])

        check_table_does_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")


class TestEmptyConfiguration(TestUnmanagedSchema):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {"managed-schemas": []}


class TestMissingConfiguration(TestUnmanagedSchema):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {}


class TestDrop(Base):
    @pytest.fixture(scope="class")
    def project_config_update(self, unique_schema):
        return {
            "managed-schemas": [
                {
                    "database": os.getenv("POSTGRES_TEST_DATABASE", "dbt"),
                    "schema": unique_schema,
                    "action": "drop",
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
                "model_b.sql": model(self.materialized),
            }
        )
        run_dbt(["manage"])

        check_table_does_not_exist(project.adapter, "model_a")
        check_table_does_exist(project.adapter, "model_b")


class TestDropView(TestDrop):
    materialized = "view"


class TestWarn(TestUnmanagedSchema):
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
