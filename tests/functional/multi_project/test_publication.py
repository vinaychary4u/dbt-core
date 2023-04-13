import pytest

from dbt.tests.util import run_dbt, get_artifact, write_file
from dbt.contracts.publication import Publication


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


dependencies_yml = """
projects:
    - name: finance
      environment: dev
    - name: marketing
      environment: dev
"""


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

        results = run_dbt(["run"])
        assert len(results) == 3
