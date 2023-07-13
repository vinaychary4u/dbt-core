from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Dict, Union

import agate

from dbt.contracts.graph.nodes import ParsedNode
from dbt.exceptions import DbtRuntimeError
from dbt.utils import filter_null_values

from dbt.adapters.relation.models._policy import RenderPolicy

"""
`Relation` metadata from the database comes in the form of a collection of one or more `agate.Table`s. In order to
reference the tables, they are added to a dictionary. There can be more than one table because there can be
multiple grains of data for a single object. For example, a materialized view in Postgres has base level information,
like name. But it also can have multiple indexes, which needs to be a separate query. The metadata for
a materialized view might look like this:

{
    "materialized_view": agate.Table(
        agate.Row({"table_name": "table_abc", "query": "select * from table_def"})
    ),
    "indexes": agate.Table("rows": [
        agate.Row({"name": "index_a", "columns": ["column_a"], "type": "hash", "unique": False}),
        agate.Row({"name": "index_b", "columns": ["time_dim_a"], "type": "btree", "unique": False}),
    ]),
}

whereas the metadata that gets used to create an index (`RelationComponent`) may look like this:

agate.Row({"name": "index_a", "columns": ["column_a"], "type": "hash", "unique": False})

Generally speaking, `Relation` instances (e.g. materialized view) will be described with
an `agate.Table` and `RelationComponent` instances (e.g. index) will be described with an `agate.Row`.
This happens simply because the `Relation` instance is the first step in processing the metadata, but the
`RelationComponent` instance can be looped when dispatching to it in `parse_describe_relation_results()`.
"""
DescribeRelationResults = Union[Dict[str, agate.Table], agate.Row]


@dataclass(frozen=True)
class RelationComponent(ABC):
    """
    This config identifies the minimal relation parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    # configuration
    render: RenderPolicy = field(compare=False)

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "RelationComponent":
        """
        This assumes the subclass of `Relation` is flat, in the sense that no attribute is
        itself another subclass of `Relation`. If that's not the case, this should be overriden
        to manually manage that complexity. But remember to either call `super().from_dict()` at the end,
        or at least use `filter_null_values()` so that defaults get applied properly for the dataclass.

        Args:
            config_dict: the dict representation of this instance

        Returns: the `Relation` representation associated with the provided dict
        """
        # default configuration
        kwargs_dict = {"render": getattr(cls, "render", RenderPolicy())}

        kwargs_dict.update(config_dict)

        try:
            relation_component = cls(**filter_null_values(kwargs_dict))  # type: ignore
        except TypeError:
            raise DbtRuntimeError(f"Unexpected configuration received:\n" f"    {config_dict}\n")
        return relation_component

    @classmethod
    def from_node(cls, node: ParsedNode) -> "RelationComponent":
        """
        A wrapper around `parse_node()` and `from_dict()` that pipes the results of the first into
        the second. This shouldn't really need to be overridden; instead, the component methods should be overridden.

        Args:
            node: the `config.model` attribute in the global jinja context

        Returns:
            a validated `Relation` instance specific to the adapter and relation type
        """
        relation_config = cls.parse_node(node)
        relation = cls.from_dict(relation_config)
        return relation

    @classmethod
    @abstractmethod
    def parse_node(cls, node: ParsedNode) -> Dict[str, Any]:
        """
        Parse `ParsedNodeMandatory` into a dict representation of a `Relation` instance

        In many cases this may be a one-to-one mapping; e.g. dbt calls it "schema" and the database calls it
        "schema_name". In some cases it could require a calculation or dispatch to a lower grain object.

        See `dbt/adapters/postgres/relation/index.py` to see an example implementation.

        Args:
            node: the `model` attribute in the global jinja context

        Returns:
            a non-validated dictionary version of a `Relation` instance specific to the adapter and relation type
        """
        raise NotImplementedError(
            "`parse_node_config()` needs to be implemented for this relation."
        )

    @classmethod
    def from_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> "RelationComponent":
        """
        A wrapper around `parse_describe_relation_results()` and `from_dict()` that pipes the results of the
        first into the second. This shouldn't really need to be overridden; instead, the component methods should
        be overridden.

        Args:
            describe_relation_results: the results of one or more queries run against the database to gather the
                requisite metadata to describe this relation

        Returns:
            a validated `Relation` instance specific to the adapter and relation type
        """
        config_dict = cls.parse_describe_relation_results(describe_relation_results)
        relation = cls.from_dict(config_dict)
        return relation

    @classmethod
    @abstractmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        """
        The purpose of this method is to parse the database parlance for `Relation.from_dict` consumption.

        This tends to be one-to-one except for combining grains of data. For example, a single table
        could have multiple indexes which would result in multiple queries to the database to build one
        `TableRelation` object. All of these pieces get knit together here.

        See `dbt/adapters/postgres/relation_config/materialized_view.py` to see an example implementation.

        Args:
            describe_relation_results: the results of one or more queries run against the database to gather the
                requisite metadata to describe this relation

        Returns:
            a non-validated dictionary version of a `Relation` instance specific to the adapter and relation type
        """
        raise NotImplementedError(
            "`parse_describe_relation_results()` needs to be implemented for this relation."
        )
