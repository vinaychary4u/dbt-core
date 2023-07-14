"""
This module provides a way to store only the required metadata for a `Relation` without any parsers or actual
relation_type-specific subclasses. It's primarily used to represent a relation that exists in the database
without needing to query the database. This is useful with low attribution macros (e.g. `drop_sql`, `rename_sql`)
where the details are not needed to perform the action. It should be the case that if a macro supports execution
with a `RelationRef` instance, then it should also support execution with a `Relation` instance. The converse
is not true (e.g. `create_sql`).
"""
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict

from dbt.contracts.graph.nodes import ParsedNode

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
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        return {}

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        return {}


@dataclass(frozen=True)
class SchemaRelationRef(SchemaRelation):
    render: RenderPolicy
    DatabaseParser = DatabaseRelationRef

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SchemaRelationRef":
        database_dict = deepcopy(config_dict["database"])
        database_dict.update({"render": config_dict["render"]})
        schema_ref = cls(
            **{
                "name": config_dict["name"],
                "database": DatabaseRelationRef.from_dict(database_dict),
                "render": config_dict["render"],
            }
        )
        assert isinstance(schema_ref, SchemaRelationRef)
        return schema_ref

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        return {}

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        return {}


@dataclass(frozen=True)
class RelationRef(Relation):
    can_be_renamed: bool
    SchemaParser = SchemaRelationRef

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "RelationRef":
        schema_dict = deepcopy(config_dict["schema"])
        schema_dict.update({"render": config_dict["render"]})
        relation_ref = cls(
            **{
                "name": config_dict["name"],
                "schema": SchemaRelationRef.from_dict(schema_dict),
                "query": "",
                "render": config_dict["render"],
                "type": config_dict["type"],
                "can_be_renamed": config_dict["can_be_renamed"],
            }
        )
        assert isinstance(relation_ref, RelationRef)
        return relation_ref

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        return {}

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        return {}
