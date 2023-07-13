from collections import OrderedDict
from dataclasses import dataclass
from typing import Any, Dict, Type, Optional

from dbt.contracts.graph.nodes import ParsedNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.relation.models._relation_component import (
    DescribeRelationResults,
    RelationComponent,
)
from dbt.adapters.relation.models._database import DatabaseRelation


@dataclass(frozen=True)
class SchemaRelation(RelationComponent):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str
    database: DatabaseRelation

    # configuration of base class
    DatabaseParser: Type[DatabaseRelation]

    @classmethod
    def _default_database_parser(cls) -> Type[DatabaseRelation]:
        return getattr(cls, "DatabaseParser", DatabaseRelation)

    def __str__(self) -> str:
        return self.fully_qualified_path or ""

    @property
    def fully_qualified_path(self) -> Optional[str]:
        return self.render.full(
            OrderedDict(
                {
                    ComponentName.Database: self.database_name,
                    ComponentName.Schema: self.name,
                }
            )
        )

    @property
    def database_name(self) -> str:
        return self.database.name

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "SchemaRelation":
        """
        Parse `config_dict` into a `SchemaRelation` instance, applying defaults
        """
        # default configuration
        kwargs_dict: Dict[str, Any] = {
            "DatabaseParser": cls._default_database_parser(),
        }

        kwargs_dict.update(config_dict)

        if database := config_dict.get("database"):
            database_parser = kwargs_dict["DatabaseParser"]
            kwargs_dict.update({"database": database_parser.from_dict(database)})

        schema = super().from_dict(kwargs_dict)
        assert isinstance(schema, SchemaRelation)
        return schema

    @classmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        """
        Parse `ParsedMandatoryNode` into a dict representation of a `SchemaRelation` instance

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more useful

        Args:
            node: the `model` attribute in the global jinja context

        Example `model_node`:

        ModelNode({
            "database": "my_database",
            "schema": "my_schema",
            ...,
        })

        Returns: a `SchemaRelation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = {
            "name": node.schema,
            "database": cls._default_database_parser().parse_node(node),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        """
        Parse database metadata into a dict representation of a `SchemaRelation` instance

        This is generally used indirectly by calling `from_describe_relation_results()`,
        but there are times when the dict version is more appropriate.

        Args:
            describe_relation_results: the results of a set of queries that fully describe an instance of this class

        Example of `describe_relation_results`:

        agate.Row({
            "schema_name": "my_schema",
            "database_name": "my_database",
        })

        Returns: a `SchemaRelation` instance as a dict, can be passed into `from_dict`
        """
        relation = cls._parse_single_record_from_describe_relation_results(
            describe_relation_results, "relation"
        )
        config_dict = {
            "name": relation["schema_name"],
            "database": cls._default_database_parser().parse_describe_relation_results(
                describe_relation_results
            ),
        }
        return config_dict
