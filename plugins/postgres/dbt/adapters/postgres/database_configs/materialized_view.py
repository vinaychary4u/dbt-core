from dataclasses import dataclass, field
from typing import Optional, Union, Set

import agate
from dbt.contracts.graph.nodes import ModelNode
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.database_configs.index import IndexConfig, IndexChange
from dbt.adapters.postgres.database_configs.object_metadata import ObjectMetadata


@dataclass
class MaterializedViewConfig:
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

    table_name: Optional[str] = None
    query: Optional[str] = None
    indexes: Set[IndexConfig] = field(default_factory=set)

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "MaterializedViewConfig":
        node_config = model_node.config

        kwargs = {}

        table_name = model_node.identifier
        if table_name:
            kwargs.update({"table_name": table_name})

        query = model_node.compiled_code
        if query:
            kwargs.update({"query": query})

        # create index objects for each index found in the config
        indexes: Optional[list] = node_config.extra.get("indexes")
        kwargs.update({"indexes": cls._format_model_node_indexes(indexes)})

        try:
            materialized_view = MaterializedViewConfig(**kwargs)
            assert materialized_view.is_valid
        except (ValueError, KeyError, AssertionError):
            raise DbtRuntimeError(f"Unexpected metadata retrieved from the config: {node_config}")
        return materialized_view

    @classmethod
    def from_database_config(cls, database_metadata: ObjectMetadata):
        # parse the agate objects into a standard dictionary
        config = cls._parse_database_config(database_metadata)

        # pull out base level supported parameters
        kwargs = {k: v for k, v in config.items() if k in ["table_name", "query"]}

        # create index objects for each index found in the config
        indexes: Optional[set] = config.pop("indexes", None)
        kwargs.update({"indexes": cls._format_database_config_indexes(indexes)})

        try:
            materialized_view = MaterializedViewConfig(**kwargs)
            assert materialized_view.is_valid
        except (ValueError, KeyError, AssertionError):
            raise DbtRuntimeError(
                f"Unexpected metadata retrieved from the database: {database_metadata}"
            )
        return materialized_view

    @classmethod
    def _parse_database_config(cls, database_metadata: ObjectMetadata) -> dict:
        """
        Parses agate objects into a standard dictionary and pushes base data to the first level

        Args:
            database_metadata: a dictionary of mixed python native and agate types

        Returns: a dictionary of python native types
        """
        base: agate.Row = database_metadata.get("base", {})
        indexes: agate.Table = database_metadata.get("indexes")

        config = dict(base)
        if indexes:
            config.update({"indexes": [dict(index) for index in indexes.rows]})

        clean_config = {k: v for k, v in config.items() if v is not None}
        return clean_config

    @classmethod
    def _format_model_node_indexes(cls, indexes: Optional[Union[set, list]]) -> Set[IndexConfig]:
        indexes_clean = set()
        if indexes:
            for index in indexes:
                index_clean = IndexConfig.from_model_node(index)
                indexes_clean.add(index_clean)
        return indexes_clean

    @classmethod
    def _format_database_config_indexes(
        cls, indexes: Optional[Union[set, list]]
    ) -> Set[IndexConfig]:
        indexes_clean = set()
        if indexes:
            for index in indexes:
                index_clean = IndexConfig.from_database_config(index)
                indexes_clean.add(index_clean)
        return indexes_clean

    @property
    def is_valid(self) -> bool:
        if self.indexes:
            return all(index.is_valid for index in self.indexes)
        return True


@dataclass(frozen=True)
class MaterializedViewChanges:
    indexes: Optional[Set[IndexChange]] = None

    @property
    def requires_full_refresh(self) -> bool:
        # no relation-level monitored changes require a full refresh
        return False
