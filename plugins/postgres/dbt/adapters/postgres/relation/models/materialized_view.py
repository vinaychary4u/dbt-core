from copy import deepcopy
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, Optional, Set

import agate
from dbt.adapters.relation.models import Relation, RelationChangeset
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation.models.index import (
    index_config_changes,
    PostgresIndexRelation,
    PostgresIndexRelationChange,
)
from dbt.adapters.postgres.relation.models.policy import (
    PostgresRenderPolicy,
    MAX_CHARACTERS_IN_IDENTIFIER,
)
from dbt.adapters.postgres.relation.models.schema import PostgresSchemaRelation


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresMaterializedViewRelation(Relation, ValidationMixin):
    """
    This config follows the specs found here:
    https://www.postgresql.org/docs/current/sql-creatematerializedview.html

    The following parameters are configurable by dbt:
    - name: name of the materialized view
    - schema: schema that contains the materialized view
    - query: the query that defines the view
    - indexes: the collection (set) of indexes on the materialized view

    Applicable defaults for non-configurable parameters:
    - method: `heap`
    - tablespace_name: `default_tablespace`
    - with_data: `True`
    """

    # attribution
    name: str
    schema: PostgresSchemaRelation
    query: str = field(hash=False, compare=False)
    indexes: Optional[FrozenSet[PostgresIndexRelation]] = field(default_factory=frozenset)

    # configuration
    type = RelationType.MaterializedView
    render = PostgresRenderPolicy
    SchemaParser = PostgresSchemaRelation
    can_be_renamed = True

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        """
        Validation rules at the materialized view level. All attribute level rules get run as a result of
        `ValidationMixin`.

        Returns: a set of rules that should evaluate to `True` (i.e. False == validation failure)
        """
        return {
            ValidationRule(
                validation_check=self.name is None
                or len(self.name) <= MAX_CHARACTERS_IN_IDENTIFIER,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than the max allowed length"
                    f"of {MAX_CHARACTERS_IN_IDENTIFIER} characters.\n"
                    f"    name: {self.name}\n"
                    f"    characters: {len(self.name)}\n"
                ),
            ),
            ValidationRule(
                validation_check=all({self.database_name, self.schema_name, self.name}),
                validation_error=DbtRuntimeError(
                    f"dbt-postgres requires all three parts of an object's path, received:\n"
                    f"    database: {self.database_name}\n"
                    f"    schema: {self.schema_name}\n"
                    f"    identifier: {self.name}\n"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PostgresMaterializedViewRelation":
        """
        Creates an instance of this class given the dict representation

        This is generally used indirectly by calling either `from_model_node()` or `from_relation_results()`

        Args:
            config_dict: a dict that aligns with the structure of this class, and it's attribute classes (e.g. indexes)

        Returns: an instance of this class
        """
        # don't alter the incoming config
        kwargs_dict = deepcopy(config_dict)

        # adapter-specific attributes
        if indexes := config_dict.get("indexes"):
            kwargs_dict.update(
                {
                    "indexes": frozenset(
                        PostgresIndexRelation.from_dict(index) for index in indexes
                    ),
                }
            )

        materialized_view = super().from_dict(kwargs_dict)
        assert isinstance(materialized_view, PostgresMaterializedViewRelation)
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Parse a `ModelNode` instance into a `PostgresMaterializedViewRelation` instance as a dict

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more appropriate.

        Args:
            model_node: the `model` attribute (e.g. `config.model`) in the jinja context

        Example `model_node`:

        ModelNode({
            "compiled_code": "create materialized view my_materialized_view as select * from my_table;",
            "config" {
                "extra": {
                    "indexes": [{"columns": ["id"], "type": "hash", "unique": True},...],
                    ...,
                },
                ...,
            },
            "database": "my_database",
            "identifier": "my_materialized_view",
            "schema": "my_schema",
            ...,
        })

        Returns: a `PostgresMaterializedViewRelation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = super().parse_model_node(model_node)

        if indexes := model_node.config.extra.get("indexes"):
            config_dict.update(
                {
                    "indexes": [
                        PostgresIndexRelation.parse_model_node(index) for index in indexes
                    ],
                }
            )

        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> dict:
        """
        Parse `RelationResults` into a dict representation of a `PostgresMaterializedViewConfig` instance

        This is generally used indirectly by calling `from_relation_results()`, but there are times when the dict
        version is more appropriate.

        Args:
            describe_relation_results: the results of a set of queries that fully describe an instance of this class

        Example of `relation_results`:

        {
            "materialization": agate.Table(agate.Row({
                "database": "my_database",
                "name": "my_materialized_view",
                "query": "create materialized view my_materialized_view as select * from my_ref_table;",
                "schema": "my_schema",
            })),
            "indexes": agate.Table([
                agate.Row({"columns": ["id"], "type": "hash", "unique": True}),
                ...,
            ],
        }

        Returns: a dict representation of an instance of this class that can be passed into `from_dict()`
        """
        config_dict = super().parse_describe_relation_results(describe_relation_results)

        if indexes := describe_relation_results.get("indexes"):
            config_dict.update(
                {
                    "indexes": [
                        PostgresIndexRelation.parse_describe_relation_results(index)
                        for index in indexes.rows
                    ],
                }
            )

        return config_dict


@dataclass
class PostgresMaterializedViewRelationChangeset(RelationChangeset):
    indexes: Set[PostgresIndexRelationChange] = field(default_factory=set)

    @classmethod
    def parse_relations(cls, existing_relation: Relation, target_relation: Relation) -> dict:
        try:
            assert isinstance(existing_relation, PostgresMaterializedViewRelation)
            assert isinstance(target_relation, PostgresMaterializedViewRelation)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two Postgres materialized view relations were expected, but received:\n"
                f"    existing: {existing_relation}\n"
                f"    new: {target_relation}\n"
            )

        config_dict = {
            "indexes": index_config_changes(existing_relation.indexes, target_relation.indexes),
        }

        return config_dict

    @property
    def requires_full_refresh(self) -> bool:
        return (
            any(index.requires_full_refresh for index in self.indexes)
            or super().requires_full_refresh
        )

    @property
    def is_empty(self) -> bool:
        return self.indexes == set() and super().is_empty
