from typing import Dict

import agate
import pytest

from dbt.adapters.materialization import MaterializationFactory
from dbt.adapters.materialization.models import (
    MaterializationType,
    MaterializedViewMaterialization,
)
from dbt.adapters.relation import RelationFactory
from dbt.adapters.relation.models import (
    MaterializedViewRelation,
    MaterializedViewRelationChangeset,
    Relation,
    RelationRef,
    RenderPolicy,
)
from dbt.contracts.files import FileHash
from dbt.contracts.graph.nodes import CompiledNode, DependsOn, NodeConfig
from dbt.contracts.relation import RelationType
from dbt.node_types import NodeType


@pytest.fixture
def relation_factory() -> RelationFactory:
    return RelationFactory(
        relation_models={
            RelationType.MaterializedView: MaterializedViewRelation,
        },
        relation_changesets={
            RelationType.MaterializedView: MaterializedViewRelationChangeset,
        },
        relation_can_be_renamed=set(),
        render_policy=RenderPolicy(),
    )


@pytest.fixture
def materialization_factory(relation_factory) -> MaterializationFactory:
    return MaterializationFactory(
        relation_factory=relation_factory,
        materialization_map={
            MaterializationType.MaterializedView: MaterializedViewMaterialization,
        },
    )


@pytest.fixture
def materialized_view_ref(relation_factory) -> RelationRef:
    return relation_factory.make_ref(
        name="my_materialized_view",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=RelationType.MaterializedView,
    )


@pytest.fixture
def view_ref(relation_factory) -> RelationRef:
    return relation_factory.make_ref(
        name="my_view",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=RelationType.View,
    )


@pytest.fixture
def materialized_view_compiled_node() -> CompiledNode:
    return CompiledNode(
        alias="my_materialized_view",
        name="my_materialized_view",
        database="my_database",
        schema="my_schema",
        unique_id="model.root.my_materialized_view",
        resource_type=NodeType.Model,
        fqn=["root", "my_materialized_view"],
        package_name="root",
        original_file_path="my_materialized_view.sql",
        refs=[],
        sources=[],
        depends_on=DependsOn(),
        config=NodeConfig.from_dict(
            {
                "enabled": True,
                "materialized": "materialized_view",
                "persist_docs": {},
                "post-hook": [],
                "pre-hook": [],
                "vars": {},
                "quoting": {},
                "column_types": {},
                "tags": [],
                "full_refresh": True,
                "on_configuration_change": "continue",
            }
        ),
        tags=[],
        path="my_materialized_view.sql",
        language="sql",
        raw_code="select 42 from meaning_of_life",
        compiled_code="select 42 from meaning_of_life",
        description="",
        columns={},
        checksum=FileHash.from_contents(""),
    )


@pytest.fixture
def materialized_view_describe_relation_results() -> Dict[str, agate.Table]:
    materialized_view_agate = agate.Table.from_object(
        [
            {
                "name": "my_materialized_view",
                "schema_name": "my_schema",
                "database_name": "my_database",
                "query": "select 42 from meaning_of_life",
            }
        ]
    )
    return {"relation": materialized_view_agate}


@pytest.fixture
def materialized_view_relation(
    relation_factory, materialized_view_describe_relation_results
) -> Relation:
    return relation_factory.make_from_describe_relation_results(
        materialized_view_describe_relation_results, RelationType.MaterializedView
    )


"""
Make sure the fixtures at least work, more thorough testing is done elsewhere
"""


def test_relation_factory(relation_factory):
    assert (
        relation_factory._get_relation_class(RelationType.MaterializedView)
        == MaterializedViewRelation
    )


def test_materialization_factory(materialization_factory):
    postgres_parser = materialization_factory.relation_factory._get_relation_model(
        RelationType.MaterializedView
    )
    assert postgres_parser == MaterializedViewRelation


def test_materialized_view_ref(materialized_view_ref):
    assert materialized_view_ref.name == "my_materialized_view"


def test_materialized_view_model_node(materialized_view_compiled_node):
    assert materialized_view_compiled_node.name == "my_materialized_view"


def test_materialized_view_relation(materialized_view_relation):
    assert materialized_view_relation.type == RelationType.MaterializedView
    assert materialized_view_relation.name == "my_materialized_view"
