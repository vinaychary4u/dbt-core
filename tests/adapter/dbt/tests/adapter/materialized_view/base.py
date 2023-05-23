from typing import List, Tuple, Optional
import os

import pytest

from dbt.tests.util import run_dbt, get_manifest, run_dbt_and_capture
from dbt.contracts.relation import RelationType


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


def get_records(project, model: str) -> List[tuple]:
    sql = f"select * from {project.database}.{project.test_schema}.{model};"
    return [tuple(row) for row in project.run_sql(sql, fetch="all")]


def get_row_count(project, model: str) -> int:
    sql = f"select count(*) from {project.database}.{project.test_schema}.{model};"
    return project.run_sql(sql, fetch="one")


def insert_record(project, record: tuple, model: str, columns: List[str]):
    sql = f"""
    insert into {project.database}.{project.test_schema}.{model} ({', '.join(columns)})
    values ({','.join(str(value) for value in record)})
    ;"""
    project.run_sql(sql)


def assert_relation_is_materialized_view(project, model: str):
    manifest = get_manifest(project.project_root)
    model_metadata = manifest.nodes[f"model.test.{model}"]
    assert model_metadata.config.materialized == RelationType.MaterializedView
    assert len(get_records(project, model)) >= 0


class Base:
    @pytest.fixture(scope="function", autouse=True)
    def setup(self, project):
        run_dbt(["run"])

    @pytest.fixture(scope="class", autouse=True)
    def project(self, project):
        yield project
