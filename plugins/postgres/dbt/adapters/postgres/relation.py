from dataclasses import dataclass
from typing import Optional, Union, Set

from dbt.adapters.base.relation import BaseRelation
from dbt.context.providers import RuntimeConfigObject
from dbt.contracts.relation import RelationType, ChangeAction
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.database_configs import (
    IndexConfig,
    IndexChange,
    MaterializedViewConfig,
    MaterializedViewChanges,
    ObjectMetadata,
)


RelationChanges = Union[MaterializedViewChanges]


@dataclass(frozen=True, eq=False, repr=False)
class PostgresRelation(BaseRelation):
    def __post_init__(self):
        # Check for length of Postgres table/view names.
        # Check self.type to exclude test relation identifiers
        if (
            self.identifier is not None
            and self.type is not None
            and len(self.identifier) > self.relation_max_name_length()
        ):
            raise DbtRuntimeError(
                f"Relation name '{self.identifier}' "
                f"is longer than {self.relation_max_name_length()} characters"
            )

    def relation_max_name_length(self):
        return 63

    def get_relation_changes(
        self, database_config: ObjectMetadata, runtime_config: RuntimeConfigObject
    ) -> Optional[RelationChanges]:
        model_node = runtime_config.model
        node_config = model_node.config
        relation_type = node_config.get("materialized", RelationType.default())
        if relation_type == RelationType.MaterializedView:
            return self._get_materialized_view_changes(database_config, runtime_config)
        return None

    def _get_materialized_view_changes(
        self, database_config: ObjectMetadata, runtime_config: RuntimeConfigObject
    ) -> Optional[MaterializedViewChanges]:
        """
        The only tracked changes for materialized views are indexes.

        Args:
            database_config:
            node_config:

        Returns:

        """
        existing_materialized_view = MaterializedViewConfig.from_database_config(database_config)
        new_materialized_view = MaterializedViewConfig.from_model_node(runtime_config.model)

        changes = {}
        index_changes = self._get_index_changes(
            existing_materialized_view.indexes, new_materialized_view.indexes
        )
        if index_changes:
            changes.update({"indexes": index_changes})

        if changes:
            return MaterializedViewChanges(**changes)
        return None

    def _get_index_changes(
        self, existing_indexes: Set[IndexConfig], new_indexes: Set[IndexConfig]
    ) -> Set[IndexChange]:
        """
        Get the index updates that will occur as a result of a new run

        There are four scenarios:

        1. Indexes are equal -> don't return these
        2. Index is new -> create these
        3. Index is old -> drop these
        4. Indexes are not equal -> drop old, create new -> two actions

        Returns: a set of index updates in the form {"action": "drop/create", "context": <IndexConfig>}
        """
        drop_indexes = existing_indexes.difference(new_indexes)
        create_indexes = new_indexes.difference(existing_indexes)

        drop_changes = set(IndexChange(ChangeAction.drop, index) for index in drop_indexes)
        create_changes = set(IndexChange(ChangeAction.create, index) for index in create_indexes)
        return drop_changes.union(create_changes)
