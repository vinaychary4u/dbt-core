from abc import ABC
from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Type

import agate
from dbt.contracts.graph.nodes import CompiledNode
from dbt.contracts.relation import ComponentName, RelationType

from dbt.adapters.relation.models._relation_component import RelationComponent
from dbt.adapters.relation.models._schema import SchemaRelation


@dataclass(frozen=True)
class Relation(RelationComponent, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    # attribution
    name: str
    schema: SchemaRelation
    query: str

    # configuration
    type: RelationType
    can_be_renamed: bool
    SchemaParser: Type[SchemaRelation]

    @classmethod
    def _default_schema_parser(cls) -> Type[SchemaRelation]:
        return getattr(cls, "SchemaParser", SchemaRelation)

    def __str__(self) -> str:
        return self.fully_qualified_path

    @property
    def fully_qualified_path(self) -> str:
        return self.render.full(
            OrderedDict(
                {
                    ComponentName.Database: self.database_name,
                    ComponentName.Schema: self.schema_name,
                    ComponentName.Identifier: self.name,
                }
            )
        )

    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def database_name(self) -> str:
        return self.schema.database_name

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "Relation":
        """
        Parse `config_dict` into a `MaterializationViewRelation` instance, applying defaults
        """
        # default configuration
        kwargs_dict: Dict[str, Any] = {
            "type": cls.type,
            "can_be_renamed": cls.can_be_renamed,
            "SchemaParser": cls._default_schema_parser(),
        }

        kwargs_dict.update(config_dict)

        if schema := config_dict.get("schema"):
            schema_parser: Type[SchemaRelation] = kwargs_dict["SchemaParser"]
            kwargs_dict.update({"schema": schema_parser.from_dict(schema)})

        relation = super().from_dict(kwargs_dict)
        assert isinstance(relation, Relation)
        return relation

    @classmethod
    def parse_node(cls, node: CompiledNode) -> Dict[str, Any]:  # type: ignore
        """
        Parse `CompiledNode` into a dict representation of a `Relation` instance

        This is generally used indirectly by calling `from_node()`, but there are times when the dict
        version is more useful

        Args:
            node: the `model` attribute in the global jinja context

        Example `node`:

        NodeConfig({
            "compiled_code": "create view my_view as\n    select * from my_table;\n",
            "database": "my_database",
            "identifier": "my_view",
            "schema": "my_schema",
            ...,
        })

        Returns: a `Relation` instance as a dict, can be passed into `from_dict`
        """
        # we need a `CompiledNode` here instead of just `ParsedNodeMandatory` because we need access to `query`
        config_dict = {
            "name": node.identifier,
            "schema": cls._default_schema_parser().parse_node(node),
            "query": (node.compiled_code or "").strip(),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(  # type: ignore
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> Dict[str, Any]:
        """
        Parse database metadata into a dict representation of a `Relation` instance

        This is generally used indirectly by calling `from_describe_relation_results()`,
        but there are times when the dict version is more appropriate.

        Args:
            describe_relation_results: the results of a set of queries that fully describe an instance of this class

        Example of `describe_relation_results`:

        {
            "relation": agate.Table(agate.Row({
                "table_name": "my_materialized_view",
                "query": "create materialized view my_materialized_view as select * from my_table;",
            })),
        }

        Returns: a `Relation` instance as a dict, can be passed into `from_dict`
        """
        relation: agate.Row = describe_relation_results["relation"].rows[0]

        config_dict = {
            "name": relation["name"],
            "schema": cls._default_schema_parser().parse_describe_relation_results(relation),
            "query": relation["query"].strip(),
        }
        return config_dict
