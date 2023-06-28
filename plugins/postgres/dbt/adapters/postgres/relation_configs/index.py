from dataclasses import dataclass, field
from typing import Set, FrozenSet

import agate
from dbt.adapters.relation_configs import (
    RelationConfig,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
    RelationConfigChangeAction,
    RelationConfigChange,
)
from dbt.contracts.relation import ComponentName
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation_configs.policy import postgres_conform_part


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
class PostgresIndexConfig(RelationConfig, RelationConfigValidationMixin):
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

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.column_names is not None,
                validation_error=DbtRuntimeError(
                    "Indexes require at least one column, but none were provided"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict) -> "PostgresIndexConfig":
        kwargs_dict = {
            "name": config_dict.get("name"),
            "column_names": frozenset(
                postgres_conform_part(ComponentName.Identifier, column)
                for column in config_dict.get("column_names", set())
            ),
            "unique": config_dict.get("unique"),
        }

        if method := config_dict.get("method"):
            kwargs_dict.update({"method": PostgresIndexMethod(method)})

        index: "PostgresIndexConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return index

    @classmethod
    def parse_model_node(cls, model_node_entry: dict) -> dict:
        config_dict = {
            "column_names": set(model_node_entry.get("columns", set())),
            "unique": model_node_entry.get("unique"),
            "method": model_node_entry.get("type"),
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
class PostgresIndexConfigChange(RelationConfigChange, RelationConfigValidationMixin):
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

    context: PostgresIndexConfig

    @property
    def requires_full_refresh(self) -> bool:
        return False

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=self.action
                in {RelationConfigChangeAction.create, RelationConfigChangeAction.drop},
                validation_error=DbtRuntimeError(
                    "Invalid operation, only `drop` and `create` changes are supported for indexes."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.drop and self.context.name is None
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operation, attempting to drop an index with no name."
                ),
            ),
            RelationConfigValidationRule(
                validation_check=not (
                    self.action == RelationConfigChangeAction.create
                    and self.context.column_names == set()
                ),
                validation_error=DbtRuntimeError(
                    "Invalid operations, attempting to create an index with no columns."
                ),
            ),
        }
