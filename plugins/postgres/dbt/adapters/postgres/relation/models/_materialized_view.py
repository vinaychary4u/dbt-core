from dataclasses import dataclass, field
from typing import Any, Dict, FrozenSet, List, Set

import agate
from dbt.adapters.relation.models import DescribeRelationResults, RelationComponent
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation.models._index import (
    PostgresIndexRelation,
    PostgresIndexRelationChange,
)
from dbt.adapters.postgres.relation.models._policy import MAX_CHARACTERS_IN_IDENTIFIER


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresMaterializedViewRelation(RelationComponent, ValidationMixin):
    """
    This config follows the specs found here:
    https://www.postgresql.org/docs/current/sql-creatematerializedview.html

    The following parameters are configurable by dbt:
    - table_name: name of the materialized view
    - query: the query that defines the view
    - indexes: the collection (set) of indexes on the materialized view

    Applicable defaults for non-configurable parameters:
    - method: `heap`
    - tablespace_name: `default_tablespace`
    - with_data: `True`
    """

    name: str = ""
    query: str = ""
    indexes: FrozenSet[PostgresIndexRelation] = field(default_factory=frozenset)

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        # index rules get run by default with the mixin
        return {
            ValidationRule(
                validation_check=self.name is None
                or len(self.name) <= MAX_CHARACTERS_IN_IDENTIFIER,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than {MAX_CHARACTERS_IN_IDENTIFIER} "
                    f"characters: {self.name}"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "PostgresMaterializedViewRelation":
        kwargs_dict = {
            "name": config_dict.get("name"),
            "query": config_dict.get("query"),
            "indexes": frozenset(
                PostgresIndexRelation.from_dict(index) for index in config_dict.get("indexes", {})
            ),
        }
        materialized_view = super().from_dict(kwargs_dict)
        assert isinstance(materialized_view, PostgresMaterializedViewRelation)
        return materialized_view

    @classmethod
    def from_node(cls, node: ModelNode) -> "PostgresMaterializedViewRelation":
        materialized_view_config = cls.parse_node(node)
        materialized_view = cls.from_dict(materialized_view_config)
        return materialized_view

    @classmethod
    def parse_node(cls, node: ModelNode) -> Dict[str, Any]:
        indexes: List[dict] = node.config.extra.get("indexes", [])
        config_dict = {
            "name": node.identifier,
            "query": node.compiled_code,
            "indexes": [PostgresIndexRelation.parse_node(index) for index in indexes],
        }
        return config_dict

    @classmethod
    def from_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> "PostgresMaterializedViewRelation":
        materialized_view_config = cls.parse_describe_relation_results(describe_relation_results)
        materialized_view = cls.from_dict(materialized_view_config)
        return materialized_view

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> Dict[str, Any]:
        indexes: agate.Table = describe_relation_results.get("indexes", agate.Table(rows={}))
        config_dict = {
            "indexes": [
                PostgresIndexRelation.parse_describe_relation_results(index)
                for index in indexes.rows
            ],
        }
        return config_dict


@dataclass
class PostgresMaterializedViewRelationChangeset:
    indexes: Set[PostgresIndexRelationChange] = field(default_factory=set)

    @property
    def requires_full_refresh(self) -> bool:
        return any(index.requires_full_refresh for index in self.indexes)

    @property
    def has_changes(self) -> bool:
        return self.indexes != set()
