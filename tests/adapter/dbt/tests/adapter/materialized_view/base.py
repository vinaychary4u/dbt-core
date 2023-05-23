from typing import List, Tuple, Optional
import os
from dataclasses import dataclass

import pytest

from dbt.tests.util import run_dbt, get_manifest, run_dbt_and_capture
from dbt.contracts.relation import RelationType


@dataclass
class Model:
    name: str
    definition: str
    columns: List[str] = None


def run_model(
    model: str,
    run_args: Optional[List[str]] = None,
    full_refresh: bool = False,
    expect_pass: bool = True,
) -> Tuple[list, str]:
    args = ["--debug", "run", "--models", model]
    if full_refresh:
        args.append("--full-refresh")
    if run_args:
        args.extend(run_args)
    return run_dbt_and_capture(args, expect_pass=expect_pass)


def assert_message_in_logs(logs: str, message: str, expected_fail: bool = False):
    # if the logs are json strings, then 'jsonify' the message because of things like escape quotes
    if os.environ.get("DBT_LOG_FORMAT", "") == "json":
        message = message.replace(r'"', r"\"")

    if expected_fail:
        assert message not in logs
    else:
        assert message in logs


def get_records(project, model: Model) -> List[tuple]:
    sql = f"select * from {project.database}.{project.test_schema}.{model.name};"
    return [tuple(row) for row in project.run_sql(sql, fetch="all")]


def get_row_count(project, model: Model) -> int:
    sql = f"select count(*) from {project.database}.{project.test_schema}.{model.name};"
    return project.run_sql(sql, fetch="one")


def insert_record(project, record: tuple, model: Model):
    sql = f"""
    insert into {project.database}.{project.test_schema}.{model.name} ({', '.join(model.columns)})
    values ({','.join(str(value) for value in record)})
    ;"""
    project.run_sql(sql)


def assert_relation_is_materialized_view(project, model: Model):
    manifest = get_manifest(project.project_root)
    model_metadata = manifest.nodes[f"model.test.{model.name}"]
    assert model_metadata.config.materialized == RelationType.MaterializedView
    assert len(get_records(project, model)) >= 0


class Base:

    base_materialized_view = Model(
        name="base_materialized_view",
        definition="{{ config(materialized='materialized_view') }} select * from {{ ref('base_table') }}",
    )

    base_table = Model(
        name="base_table",
        definition="{{ config(materialized='table') }} select 1 as base_column",
        columns=["base_column"],
    )

    @pytest.fixture(scope="class")
    def models(self):
        return {
            f"{model.name}.sql": model.definition
            for model in [self.base_table, self.base_materialized_view]
        }

    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["run"])
