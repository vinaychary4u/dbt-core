from typing import List

import pytest

from dbt.contracts.graph.manifest import Manifest
from dbt.events.base_types import BaseEvent
from dbt.tests.util import write_file
from tests.functional.assertions.test_runner import dbtTestRunner
from tests.functional.saved_queries.fixtures import saved_queries_yml
from tests.functional.semantic_models.fixtures import (
    fct_revenue_sql,
    metricflow_time_spine_sql,
    schema_yml,
)


class TestSavedQueryParsing:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "saved_queries.yml": saved_queries_yml,
            "schema.yml": schema_yml,
            "fct_revenue.sql": fct_revenue_sql,
            "metricflow_time_spine.sql": metricflow_time_spine_sql,
        }

    def test_semantic_model_parsing(self, project):
        runner = dbtTestRunner()
        result = runner.invoke(["parse"])
        result.result
        assert result.success
        assert isinstance(result.result, Manifest)
        manifest = result.result
        assert len(manifest.saved_queries) == 1
        saved_query = manifest.saved_queries["saved_query.test.test_saved_query"]
        assert saved_query.name == "test_saved_query"
        assert len(saved_query.metrics) == 1
        assert len(saved_query.group_bys) == 1
        assert len(saved_query.where) == 2
        assert len(saved_query.depends_on.nodes) == 1

    def test_saved_query_error(self, project):
        error_schema_yml = saved_queries_yml.replace("simple_metric", "metric_not_found")
        write_file(error_schema_yml, project.project_root, "models", "saved_queries.yml")
        events: List[BaseEvent] = []
        runner = dbtTestRunner(callbacks=[events.append])

        result = runner.invoke(["parse", "--no-partial-parse"])
        assert not result.success
        validation_errors = [e for e in events if e.info.name == "MainEncounteredError"]
        assert validation_errors
