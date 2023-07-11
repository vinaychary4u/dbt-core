from abc import ABC
from copy import deepcopy
from collections import OrderedDict
from dataclasses import dataclass

import agate

from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.relation.models._relation import RelationComponent
from dbt.adapters.relation.models._database import DatabaseRelation


@dataclass(frozen=True)
class SchemaRelation(RelationComponent, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str
    database: DatabaseRelation

    # configuration of base class
    DatabaseParser: DatabaseRelation

    def __str__(self) -> str:
        return self.fully_qualified_path

    @property
    def fully_qualified_path(self) -> str:
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
    def from_dict(cls, config_dict) -> "SchemaRelation":
        """
        Parse `config_dict` into a `SchemaRelation` instance, applying defaults
        """
        # don't alter the incoming config
        kwargs_dict = deepcopy(config_dict)

        # configuration
        kwargs_dict.update(
            {
                "DatabaseParser": cls.DatabaseParser,
            }
        )

        if database := config_dict.get("database"):
            kwargs_dict.update({"database": cls.DatabaseParser.from_dict(database)})

        schema = super().from_dict(kwargs_dict)
        assert isinstance(schema, SchemaRelation)
        return schema

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Parse `ModelNode` into a dict representation of a `SchemaRelation` instance

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more useful

        Args:
            model_node: the `model` (`ModelNode`) attribute (e.g. `config.model`) in the global jinja context

        Example `model_node`:

        ModelNode({
            "database": "my_database",
            "schema": "my_schema",
            ...,
        })

        Returns: a `SchemaRelation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = {
            "name": model_node.schema,
            "database": cls.DatabaseParser.parse_model_node(model_node),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:  # type: ignore
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
        config_dict = {
            "name": describe_relation_results["schema_name"],
            "database": cls.DatabaseParser.parse_describe_relation_results(
                describe_relation_results
            ),
        }
        return config_dict
