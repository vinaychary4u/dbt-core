from copy import deepcopy
from dataclasses import dataclass, field
from typing import Set, FrozenSet

import agate
from dbt.adapters.relation.models import (
    RelationComponent,
    RelationChangeAction,
    RelationChange,
)
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.contracts.relation import ComponentName
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation.models.policy import PostgresRenderPolicy


class PostgresIndexMethod(StrEnum):
    btree = "btree"
    hash = "hash"
    gist = "gist"
    spgist = "spgist"
    gin = "gin"
    brin = "brin"

    @classmethod
    def default(cls) -> "PostgresIndexMethod":
        return cls.btree


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresIndexRelation(RelationComponent, ValidationMixin):
    """
    This config fallows the specs found here:
    https://www.postgresql.org/docs/current/sql-createindex.html

    The following parameters are configurable by dbt:
    - column_names: the columns in the index
    - unique: checks for duplicate values when the index is created and on data updates
    - method: the index method to be used

    The following parameters are not configurable by dbt, but are required for certain functionality:
    - name: the name of the index in the database

    Applicable defaults for non-configurable parameters:
    - concurrently: `False`
    - nulls_distinct: `True`

    *Note: The index does not have a name until it is created in the database. The name also must be globally
    unique, not just within the materialization to which it belongs. Hence, the name is a combination of attributes
    on both the index and the materialization. This is calculated with `PostgresRelation.generate_index_name()`.
    """

    column_names: FrozenSet[str] = field(hash=True)
    name: str = field(default=None, hash=False, compare=False)
    unique: bool = field(default=False, hash=True)
    method: PostgresIndexMethod = field(default=PostgresIndexMethod.default(), hash=True)

    # configuration
    render = PostgresRenderPolicy

    @property
    def fully_qualified_path(self) -> str:
        return "_".join(
            {
                *sorted(
                    self.render.part(ComponentName.Identifier, column)
                    for column in self.column_names
                ),
                str(self.unique),
                str(self.method),
            }
        ).replace(self.render.quote_character, "")

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.column_names != frozenset(),
                validation_error=DbtRuntimeError(
                    "Indexes require at least one column, but none were provided"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "PostgresIndexRelation":
        # don't alter the incoming config
        kwargs_dict = deepcopy(config_dict)

        # component-specific attributes
        if column_names := config_dict.get("column_names"):
            kwargs_dict.update({"column_names": frozenset(column_names)})

        if method := config_dict.get("method"):
            kwargs_dict.update({"method": PostgresIndexMethod(method)})

        index = super().from_dict(kwargs_dict)
        assert isinstance(index, PostgresIndexRelation)
        return index

    @classmethod
    def parse_node(cls, node_entry: dict) -> dict:
        """
        Parse a `ModelNode` instance into a `PostgresIndexRelation` instance as a dict

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more appropriate.

        Args:
            node_entry: an entry from the `model` attribute (e.g. `config.model`) in the jinja context

        Example `model_node`:

        ModelNode({
            "config" {
                "extra": {
                    "indexes": [{"columns": ["id"], "type": "hash", "unique": True},...],
                    ...,
                },
                ...,
            },
            ...,
        })

        Returns: a `PostgresIndexRelation` instance as a dict, can be passed into `from_dict`
        """
        config_dict = {
            "column_names": set(node_entry.get("columns", set())),
            "unique": node_entry.get("unique"),
            "method": node_entry.get("type"),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        config_dict = {
            "name": describe_relation_results["name"],
            "column_names": set(describe_relation_results["column_names"].split(",")),
            "unique": describe_relation_results["unique"],
            "method": describe_relation_results["method"],
        }
        return config_dict


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresIndexRelationChange(RelationChange, ValidationMixin):
    """
    Example of an index change:
    {
        "action": "create",
        "context": {
            "name": "",  # we don't know the name since it gets created as a hash at runtime
            "columns": ["column_1", "column_3"],
            "type": "hash",
            "unique": True
        }
    },
    {
        "action": "drop",
        "context": {
            "name": "index_abc",  # we only need this to drop, but we need the rest to compare
            "columns": ["column_1"],
            "type": "btree",
            "unique": True
        }
    }
    """

    context: PostgresIndexRelation

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        return {
            ValidationRule(
                validation_check=self.action
                in {RelationChangeAction.create, RelationChangeAction.drop},
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `drop` and `create` changes are supported for indexes."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.action == RelationChangeAction.drop and self.context.name is None
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operation, attempting to drop an index with no name."
                ),
            ),
            ValidationRule(
                validation_check=not (
                    self.action == RelationChangeAction.create
                    and self.context.column_names == set()
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operations, attempting to create an index with no columns."
                ),
            ),
        }


def index_config_changes(
    existing_indexes: FrozenSet[PostgresIndexRelation],
    new_indexes: FrozenSet[PostgresIndexRelation],
) -> Set[PostgresIndexRelationChange]:
    """
    Get the index updates that will occur as a result of a new run

    There are four scenarios:

    1. Indexes are equal -> don't return these
    2. Index is new -> create these
    3. Index is old -> drop these
    4. Indexes are not equal -> drop old, create new -> two actions

    Returns: a set of index updates in the form {"action": "drop/create", "context": <IndexConfig>}
    """
    drop_changes = set(
        PostgresIndexRelationChange(action=RelationChangeAction.drop, context=index)
        for index in existing_indexes.difference(new_indexes)
    )
    create_changes = set(
        PostgresIndexRelationChange(action=RelationChangeAction.create, context=index)
        for index in new_indexes.difference(existing_indexes)
    )
    return set().union(drop_changes, create_changes)
