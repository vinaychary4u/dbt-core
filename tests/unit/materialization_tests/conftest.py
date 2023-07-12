from dataclasses import dataclass

import agate
import pytest

from dbt.adapters.materialization.factory import MaterializationFactory
from dbt.adapters.relation.factory import RelationFactory
from dbt.adapters.relation.models import MaterializedViewRelation
from dbt.contracts.files import FileHash
from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.contracts.graph.nodes import DependsOn, ModelNode, NodeConfig
from dbt.contracts.relation import RelationType
from dbt.node_types import NodeType


@pytest.fixture
def relation_factory():
    return RelationFactory()


@pytest.fixture
def materialization_factory(relation_factory):
    return MaterializationFactory()


@pytest.fixture
def view_ref(relation_factory):
    return relation_factory.make_ref(
        name="my_view",
        schema_name="my_schema",
        database_name="my_database",
        relation_type=RelationType.View,
    )


@pytest.fixture
def materialized_view_describe_relation_results():
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
def materialized_view_relation(relation_factory, materialized_view_describe_relation_results):
    return relation_factory.make_from_describe_relation_results(
        materialized_view_describe_relation_results, RelationType.MaterializedView
    )


@pytest.fixture
def materialized_view_model_node():
    return ModelNode(
        alias="my_materialized_view",
        name="my_materialized_view",
        database="my_database",
        schema="my_schema",
        resource_type=NodeType.Model,
        unique_id="model.root.my_materialized_view",
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
def materialized_view_runtime_config(materialized_view_model_node):
    """
    This is not actually a `RuntimeConfigObject`. It's an object that has attribution that looks like
    a boiled down version of a RuntimeConfigObject.

    TODO: replace this with an actual `RuntimeConfigObject`
    """

    @dataclass()
    class RuntimeConfigObject:
        model: ModelNode
        full_refresh: bool
        grants: dict
        on_configuration_change: OnConfigurationChangeOption

        def get(self, attribute: str, default=None):
            return getattr(self, attribute, default)

    return RuntimeConfigObject(
        model=materialized_view_model_node,
        full_refresh=False,
        grants={},
        on_configuration_change=OnConfigurationChangeOption.Continue,
    )


"""
Make sure the fixtures at least work, more thorough testing is done elsewhere
"""


def test_relation_factory(relation_factory):
    assert relation_factory._get_parser(RelationType.MaterializedView) == MaterializedViewRelation


def test_materialization_factory(materialization_factory):
    postgres_parser = materialization_factory.relation_factory._get_parser(
        RelationType.MaterializedView
    )
    assert postgres_parser == MaterializedViewRelation


def test_materialized_view_ref(materialized_view_ref):
    assert materialized_view_ref.name == "my_materialized_view"


def test_materialized_view_model_node(materialized_view_model_node):
    assert materialized_view_model_node.name == "my_materialized_view"


def test_materialized_view_runtime_config(materialized_view_runtime_config):
    assert materialized_view_runtime_config.get("full_refresh", False) is False
    assert materialized_view_runtime_config.get("on_configuration_change", "apply") == "continue"
    assert materialized_view_runtime_config.model.name == "my_materialized_view"


def test_materialized_view_relation(materialized_view_relation):
    assert materialized_view_relation.type == RelationType.MaterializedView
    assert materialized_view_relation.name == "my_materialized_view"
