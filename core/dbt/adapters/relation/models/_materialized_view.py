from abc import ABC
from dataclasses import dataclass
from typing import Dict

import agate

from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.relation.models._change import RelationChangeset
from dbt.adapters.relation.models._relation import Relation


@dataclass(frozen=True)
class MaterializedViewRelation(Relation, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    # attribution
    query: str

    # configuration
    # TODO: move `can_be_renamed` to `Relation`; see `Relation` for more information
    can_be_renamed: bool

    @classmethod
    def from_dict(cls, config_dict) -> "MaterializedViewRelation":
        """
        Parse `config_dict` into a `MaterializationViewRelation` instance, applying defaults
        """
        # default configuration
        kwargs_dict = {
            "type": RelationType.MaterializedView,
            "can_be_renamed": cls.can_be_renamed,
        }
        kwargs_dict.update(config_dict)

        materialized_view = super().from_dict(kwargs_dict)
        assert isinstance(materialized_view, MaterializedViewRelation)
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Parse `ModelNode` into a dict representation of a `MaterializedViewRelation` instance

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

        Returns: a `MaterializedViewRelation` instance as a dict, can be passed into `from_dict`
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
        Parse database metadata into a dict representation of a `MaterializedViewRelation` instance

        This is generally used indirectly by calling `from_describe_relation_results()`,
        but there are times when the dict version is more appropriate.

        Args:
            describe_relation_results: the results of a set of queries that fully describe an instance of this class

        Example of `describe_relation_results`:

        {
            "materialized_view": agate.Table(agate.Row({
                "table_name": "my_materialized_view",
                "query": "create materialized view my_materialized_view as select * from my_table;",
            })),
        }

        Returns: a `MaterializedViewRelation` instance as a dict, can be passed into `from_dict`
        """
        materialization: agate.Row = describe_relation_results["materialized_view"].rows[0]

        config_dict = {
            "name": materialization["name"],
            "schema": cls.SchemaParser.parse_describe_relation_results(materialization),
            "query": materialization["query"].strip(),
        }
        return config_dict


class MaterializedViewRelationChangeset(RelationChangeset):
    @classmethod
    def parse_relations(cls, existing_relation: Relation, target_relation: Relation) -> dict:
        try:
            assert isinstance(existing_relation, MaterializedViewRelation)
            assert isinstance(target_relation, MaterializedViewRelation)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two materialized view relations were expected, but received:\n"
                f"    existing: {existing_relation}\n"
                f"    new: {target_relation}\n"
            )

        return {}
