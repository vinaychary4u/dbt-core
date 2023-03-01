import pytest

from dbt.tests.util import run_dbt, get_manifest

my_model_sql = """
---
description: "testing yaml frontmatter"
config:
  tags: ['tag_my_model']
  meta:
    owner: 'Julie Smith'
    my_attr: 'something'
columns:
  - name: id
    tests:
      - unique
      - not_null
---

select 1 as id
"""


class TestYamlFrontmatter:
    @pytest.fixture(scope="class")
    def models(self):
        return {"my_model.sql": my_model_sql}

    def test_yaml_frontmatter(self, project):
        results = run_dbt(["run"])
        len(results) == 1
        manifest = get_manifest(project.project_root)
        my_model_id = "model.test.my_model"
        assert my_model_id in manifest.nodes
        assert len(manifest.nodes) == 3
        model = manifest.nodes[my_model_id]
        assert model.description == "testing yaml frontmatter"
        assert model.tags == ["tag_my_model"]
