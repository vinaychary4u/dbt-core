"""
This module provides a way to store only the required metadata for a `Relation` without any parsers or actual
relation_type-specific subclasses. It's primarily used to represent a relation that exists in the database
without needing to query the database. This is useful with low attribution macros (e.g. `drop_sql`, `rename_sql`)
where the details are not needed to perform the action. It should be the case that if a macro supports execution
with a `RelationRef` instance, then it should also support execution with a `Relation` instance. The converse
is not true (e.g. `create_sql`).
"""
from dataclasses import dataclass
from typing import Any, Dict

from dbt.contracts.graph.nodes import CompiledNode

from dbt.adapters.relation.models._database import DatabaseRelation
from dbt.adapters.relation.models._policy import RenderPolicy
from dbt.adapters.relation.models._relation import Relation
from dbt.adapters.relation.models._relation_component import DescribeRelationResults
from dbt.adapters.relation.models._schema import SchemaRelation


@dataclass(frozen=True)
class DatabaseRelationRef(DatabaseRelation):
    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "DatabaseRelationRef":
        database_ref = cls(
            **{
                "name": config_dict["name"],
                "render": config_dict["render"],
            }
        )
        assert isinstance(database_ref, DatabaseRelationRef)
        return database_ref

    @classmethod
    def parse_node(cls, node: CompiledNode) -> Dict[str, Any]:  # type: ignore
        return {}

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        return {}


@dataclass(frozen=True)
class SchemaRelationRef(SchemaRelation):
    render: RenderPolicy

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SchemaRelationRef":
        schema_ref = cls(
            **{
                "name": config_dict["name"],
                "database": DatabaseRelation.from_dict(config_dict["database"]),
                "render": config_dict["render"],
                "DatabaseParser": DatabaseRelationRef,
            }
        )
        assert isinstance(schema_ref, SchemaRelationRef)
        return schema_ref

    @classmethod
    def parse_node(cls, node: CompiledNode) -> Dict[str, Any]:  # type: ignore
        return {}

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        return {}


@dataclass(frozen=True)
class RelationRef(Relation):
    can_be_renamed: bool

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "RelationRef":
        relation_ref = cls(
            **{
                "name": config_dict["name"],
                "schema": SchemaRelationRef.from_dict(config_dict["schema"]),
                "query": "",
                "render": config_dict["render"],
                "type": config_dict["type"],
                "can_be_renamed": config_dict["can_be_renamed"],
                "SchemaParser": SchemaRelationRef,
            }
        )
        assert isinstance(relation_ref, RelationRef)
        return relation_ref

    @classmethod
    def parse_node(cls, node: CompiledNode) -> Dict[str, Any]:  # type: ignore
        return {}

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        return {}
