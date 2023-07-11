from abc import ABC
from dataclasses import dataclass

import agate

from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName

from dbt.adapters.relation.models._relation import RelationComponent


@dataclass(frozen=True)
class DatabaseRelation(RelationComponent, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str

    def __str__(self) -> str:
        return self.fully_qualified_path

    @property
    def fully_qualified_path(self) -> str:
        path = self.render.part(ComponentName.Database, self.name)
        assert isinstance(path, str)
        return path

    @classmethod
    def from_dict(cls, config_dict) -> "DatabaseRelation":
        """
        Parse `config_dict` into a `DatabaseRelation` instance, applying defaults
        """
        database = super().from_dict(config_dict)
        assert isinstance(database, DatabaseRelation)
        return database

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Parse `ModelNode` into a dict representation of a `DatabaseRelation` instance

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more useful

        Args:
            model_node: the `model` (`ModelNode`) attribute (e.g. `config.model`) in the global jinja context

        Example `model_node`:

        ModelNode({
            "database": "my_database",
            ...,
        })

        Returns: a `DatabaseRelation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = {"name": model_node.database}
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:  # type: ignore
        """
        Parse database metadata into a dict representation of a `DatabaseRelation` instance

        This is generally used indirectly by calling `from_describe_relation_results()`,
        but there are times when the dict version is more appropriate.

        Args:
            describe_relation_results: the results of a set of queries that fully describe an instance of this class

        Example of `describe_relation_results`:

        agate.Row({
            "database_name": "my_database",
        })

        Returns: a `DatabaseRelation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = {"name": describe_relation_results["database_name"]}
        return config_dict
