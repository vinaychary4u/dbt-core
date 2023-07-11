import pytest

from dbt.adapters.relation.models import RelationStub
from dbt.adapters.relation.factory import RelationFactory
from dbt.contracts.relation import RelationType

from dbt.adapters.postgres.relation import models as relation_models


@pytest.fixture(scope="class")
def relation_factory():
    return RelationFactory(
        relation_models={
            RelationType.MaterializedView: relation_models.PostgresMaterializedViewRelation,
        },
        relation_can_be_renamed={RelationType.MaterializedView},
        render_policy=relation_models.PostgresRenderPolicy,
    )


@pytest.fixture(scope="class")
def my_materialized_view(project, relation_factory) -> RelationStub:
    return relation_factory.make_stub(
        name="my_materialized_view",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=RelationType.MaterializedView,
    )


@pytest.fixture(scope="class")
def my_view(project, relation_factory) -> RelationStub:
    return relation_factory.make_stub(
        name="my_view",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=RelationType.View,
    )


@pytest.fixture(scope="class")
def my_table(project, relation_factory) -> RelationStub:
    return relation_factory.make_stub(
        name="my_table",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=RelationType.Table,
    )


@pytest.fixture(scope="class")
def my_seed(project, relation_factory) -> RelationStub:
    return relation_factory.make_stub(
        name="my_seed",
        schema_name=project.test_schema,
        database_name=project.database,
        relation_type=RelationType.Table,
    )
