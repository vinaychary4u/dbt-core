from abc import ABC
from collections import OrderedDict
from dataclasses import dataclass
from typing import Dict

import agate
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName, RelationType

from dbt.adapters.relation.models._relation_component import RelationComponent
from dbt.adapters.relation.models._schema import SchemaRelation


@dataclass(frozen=True)
class Relation(RelationComponent, ABC):

    # attribution
    name: str
    schema: SchemaRelation

    """
    TODO: `can_be_renamed` belongs on `Relation`; however, I get the error below and cannot figure out how to fix it.

        TypeError: non-default argument 'can_be_renamed' follows default argument

    """
    # configuration
    type: RelationType
    SchemaParser: SchemaRelation

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
    def from_dict(cls, config_dict) -> "Relation":
        """
        Parse `config_dict` into a `MaterializationViewRelation` instance, applying defaults
        """
        # default configuration
        kwargs_dict = {"SchemaParser": cls.SchemaParser}
        kwargs_dict.update(config_dict)

        if schema := config_dict.get("schema"):
            kwargs_dict.update({"schema": kwargs_dict["SchemaParser"].from_dict(schema)})

        relation = super().from_dict(kwargs_dict)
        assert isinstance(relation, Relation)
        return relation

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Parse `ModelNode` into a dict representation of a `Relation` instance

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more useful

        Args:
            model_node: the `model` (`ModelNode`) attribute (e.g. `config.model`) in the global jinja context

        Example `model_node`:

        ModelNode({
            "compiled_code": "create view my_view as\n    select * from my_table;\n",
            "database": "my_database",
            "identifier": "my_view",
            "schema": "my_schema",
            ...,
        })

        Returns: a `Relation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = {
            "name": model_node.identifier,
            "schema": cls.SchemaParser.parse_model_node(model_node),
            "query": (model_node.compiled_code or "").strip(),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(  # type: ignore
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> dict:
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
            "schema": cls.SchemaParser.parse_describe_relation_results(relation),
            "query": relation["query"].strip(),
        }
        return config_dict
