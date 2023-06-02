from dataclasses import dataclass, field
from enum import Enum
from typing import Optional, Set, Dict, Union, List

from dbt.exceptions import DbtRuntimeError

from dbt.contracts.relation import ChangeAction, Change


# A `NodeConfig` instance can have multiple indexes, this is just one index
# e.g. {"columns": ["column_a", "column_b"], "unique": True, "type": "hash"}
Columns = List[str]
ModelNodeEntry = Dict[str, Union[Columns, bool, str]]


class IndexMethod(str, Enum):
    btree = "btree"
    hash = "hash"
    gist = "gist"
    spgist = "spgist"
    gin = "gin"
    brin = "brin"

    @classmethod
    def default(cls) -> "IndexMethod":
        return cls.btree


@dataclass
class IndexConfig:
    """
    This config fallows the specs found here:
    https://www.postgresql.org/docs/current/sql-createindex.html

    The following parameters are configurable by dbt:
    - name: the name of the index in the database, this isn't predictable since we apply a timestamp
    - unique: checks for duplicate values when the index is created and on data updates
    - method: the index method to be used
    - column_names: the columns in the index

    Applicable defaults for non-configurable parameters:
    - concurrently: `False`
    - nulls_distinct: `True`
    """

    name: Optional[str] = None
    unique: Optional[bool] = False
    method: Optional[IndexMethod] = IndexMethod.default()
    column_names: Optional[Set[str]] = field(default_factory=set)

    @classmethod
    def from_model_node(cls, model_node_entry: ModelNodeEntry) -> "IndexConfig":
        unique = model_node_entry.get("unique")
        method = model_node_entry.get("type")
        columns = set(model_node_entry.get("columns", []))

        try:
            index_config = cls._get_valid_index_config(columns, unique, method)
        except DbtRuntimeError:
            raise DbtRuntimeError(
                f"Unexpected index metadata retrieved from the config: {model_node_entry}"
            )
        return index_config

    @classmethod
    def from_database_config(cls, database_config: dict) -> "IndexConfig":
        name = database_config.get("name")
        unique = database_config.get("unique")
        method = database_config.get("method")
        columns = set(database_config.get("column_names", "").split(","))

        try:
            index_config = cls._get_valid_index_config(columns, unique, method, name)
        except DbtRuntimeError:
            raise DbtRuntimeError(
                f"Unexpected index metadata retrieved from the database: {database_config}"
            )
        return index_config

    def as_user_config(self):
        """
        Returns:
        """
        config = {
            "columns": list(self.column_names),
            "unique": self.unique,
            "type": self.method.value,
        }
        return config

    @classmethod
    def _get_valid_index_config(
        cls,
        column_names: Optional[Set[str]],
        unique: Optional[bool] = None,
        method: Optional[IndexMethod] = None,
        name: Optional[str] = None,
    ) -> "IndexConfig":
        kwargs = {
            "unique": unique,
            "method": method,
            "name": name,
        }

        if isinstance(column_names, set) and len(column_names) > 0:
            # TODO: do we need to look at the quoting policy?
            kwargs.update({"column_names": set(column.lower() for column in column_names)})

        # ensure the defaults are set, passing in a keyword with None overrides the default on a dataclass
        kwargs = {k: v for k, v in kwargs.items() if v is not None}

        index_config = IndexConfig(**kwargs)
        if index_config.is_valid:
            return index_config
        raise DbtRuntimeError(
            f"Unexpected index metadata: columns: {column_names}; unique: {unique}; type: {method}"
        )

    @property
    def is_valid(self) -> bool:
        if self.column_names is None:
            return False
        return True

    def __eq__(self, other):
        if isinstance(other, IndexConfig):
            return all(
                {
                    self.unique == other.unique,
                    self.method == other.method,
                    self.column_names == other.column_names,
                }
            )
        return False

    def __hash__(self):
        # Allow for sets of indexes defined only by columns, type, and uniqueness; i.e. remove the timestamp
        return hash(
            (
                self.unique,
                self.method,
                frozenset(self.column_names),
            )
        )


@dataclass(frozen=True)
class IndexChange(Change):
    """
    Example of an index update:
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

    action: ChangeAction
    context: IndexConfig

    def __post_init__(self):
        if self.action == ChangeAction.drop and self.context.name is None:
            raise DbtRuntimeError("Invalid operation, attempting to drop an index with no name.")
        if self.action == ChangeAction.create and self.context.column_names == []:
            raise DbtRuntimeError(
                "Invalid operations, attempting to create an index with no columns."
            )
