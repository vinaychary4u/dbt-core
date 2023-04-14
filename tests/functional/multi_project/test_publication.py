import pytest
import pathlib

from dbt.tests.util import run_dbt, get_artifact, write_file
from dbt.contracts.publication import Publication, PublicModel
from dbt.exceptions import PublicationConfigNotFound


model_one_sql = """
select 1 as fun
"""

model_two_sql = """
select fun from {{ ref('model_one') }}
"""

model_three_sql = """
select fun from {{ ref('model_two') }}
"""

models_yml = """
models:
  - name: model_one
    description: model one
    access: public
  - name: model_two
    description: non-public model
  - name: model_three
    description: model three
    access: public
"""


dependencies_yml = """
projects:
    - name: marketing
      environment: dev
"""

marketing_pub_json = """
{
  "project_name": "marketing",
  "metadata": {
    "dbt_schema_version": "https://schemas.getdbt.com/dbt/publication/v1.json",
    "dbt_version": "1.5.0",
    "generated_at": "2023-04-13T17:17:58.128706Z",
    "invocation_id": "56e3126f-78c7-470c-8eb0-c94af7c3eaac",
    "env": {},
    "adapter_type": "postgres",
    "quoting": {
      "database": true,
      "schema": true,
      "identifier": true
    }
  },
  "public_models": {
    "model.marketing.fct_one": {
      "name": "fct_one",
      "package_name": "marketing",
      "unique_id": "model.marketing.fct_one",
      "relation_name": '"dbt"."test_schema"."fct_one"',
      "version": null,
      "is_latest_version": false,
      "public_dependencies": []
    },
    "model.marketing.fct_two": {
      "name": "fct_two",
      "package_name": "marketing",
      "unique_id": "model.marketing.fct_two",
      "relation_name": '"dbt"."test_schema"."fct_two"',
      "version": null,
      "is_latest_version": false,
      "public_dependencies": ["model.test.fct_one"]
    }
  },
  "dependencies": []
}
"""

ext_node_model_sql = """
select * from {{ ref('marketing', 'fct_one') }}
"""


class TestPublicationArtifact:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "model_two.sql": model_two_sql,
            "model_three.sql": model_three_sql,
            "models.yml": models_yml,
        }

    def test_publication_artifact(self, project):
        results = run_dbt(["run"])
        assert len(results) == 3

        publication_dict = get_artifact(project.project_root, "target", "test_publication.json")
        publication = Publication.from_dict(publication_dict)
        assert publication
        assert len(publication.public_models) == 2
        assert publication.public_models["model.test.model_three"].public_dependencies == [
            "model.test.model_one"
        ]


class TestDependenciesYml:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one_sql,
            "model_two.sql": model_two_sql,
            "model_three.sql": model_three_sql,
            "models.yml": models_yml,
        }

    def test_dependencies(self, project):
        write_file(dependencies_yml, "dependencies.yml")

        # Depdencies lists "marketing" project, but no publication file found
        with pytest.raises(PublicationConfigNotFound):
            run_dbt(["parse"])

        # Write out publication file and try again
        m_pub_json = marketing_pub_json.replace("test_schema", project.test_schema)
        (pathlib.Path(project.project_root) / "publications").mkdir(parents=True, exist_ok=True)
        write_file(m_pub_json, project.project_root, "publications", "marketing_publication.json")

        manifest = run_dbt(["parse"])
        assert manifest.publications
        assert "marketing" in manifest.publications
        public_model = manifest.publications["marketing"].public_models["model.marketing.fct_one"]
        assert public_model

        # target_model_name, target_model_package, target_model_version, current_project, node_package
        resolved_node = manifest.resolve_ref("fct_one", "marketing", None, "test", "test")
        assert resolved_node
        assert isinstance(resolved_node, PublicModel)
        assert resolved_node.unique_id == "model.marketing.fct_one"

        # add new model that references external_node
        write_file(ext_node_model_sql, project.project_root, "models", "ext_node_model.sql")
        manifest = run_dbt(["parse"])

        model_id = "model.test.ext_node_model"
        model = manifest.nodes[model_id]
        assert model.depends_on.external_nodes == ["model.marketing.fct_one"]
