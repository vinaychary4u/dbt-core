import json
import pytest

from dbt.tests.util import run_dbt

_MODELS__MODEL_USING_QUOTE_UTIL = """
select 1 as {{ adapter.quote("2id") }}
"""

_PROPERTIES__QUOTE_MODEL = """
version: 2
models:
  - name: quote_model
    description: "model to test column quotes and comments"
    columns:
      - name: 2id
        description: "XXX My description"
        quote: true
"""


class TestColumnComment:
    @pytest.fixture(scope="class")
    def models(self):
        return {"quote_model.sql": _MODELS__MODEL_USING_QUOTE_UTIL}

    @pytest.fixture(scope="class")
    def properties(self):
        return {"properties.yml": _PROPERTIES__QUOTE_MODEL}

    @pytest.fixture(scope="class")
    def project_config_update(self):
        return {
            "models": {
                "test": {
                    "materialized": "table",
                    "+persist_docs": {
                        "relation": True,
                        "columns": True,
                    },
                }
            }
        }

    @pytest.fixture(scope="class")
    def run_has_comments(self, project):
        run_dbt()
        run_dbt(["docs", "generate"])
        with open("target/catalog.json") as fp:
            catalog_data = json.load(fp)
        assert "nodes" in catalog_data
        assert len(catalog_data["nodes"]) == 1
        column_node = catalog_data["nodes"]["model.test.quote_model"]
        column_comment = column_node["columns"]["2id"]["comment"]
        assert column_comment.startswith("XXX")

    def test_postgres_comments(self, run_has_comments):
        run_has_comments
