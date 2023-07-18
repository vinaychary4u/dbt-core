from dataclasses import dataclass
from typing import Optional, Set, FrozenSet

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation.models import DescribeRelationResults, RelationChangeAction
from dbt.context.providers import RuntimeConfigObject
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation.models import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    PostgresIndexRelation,
    PostgresIndexRelationChange,
    PostgresMaterializedViewRelation,
    PostgresMaterializedViewRelationChangeset,
)


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
        return MAX_CHARACTERS_IN_IDENTIFIER

    def materialized_view_relation_changeset(
        self, relation_results: DescribeRelationResults, runtime_config: RuntimeConfigObject
    ) -> Optional[PostgresMaterializedViewRelationChangeset]:
        config_change_collection = PostgresMaterializedViewRelationChangeset()

        existing_materialized_view = (
            PostgresMaterializedViewRelation.from_describe_relation_results(relation_results)
        )
        new_materialized_view = PostgresMaterializedViewRelation.from_node(runtime_config.model)

        config_change_collection.indexes = self._index_relation_changes(
            existing_materialized_view.indexes, new_materialized_view.indexes
        )

        # we return `None` instead of an empty `PostgresMaterializedViewConfigChangeCollection` object
        # so that it's easier and more extensible to check in the materialization:
        # `core/../materializations/materialized_view.sql` :
        #     {% if configuration_changes is none %}
        if config_change_collection.has_changes:
            return config_change_collection

    def _index_relation_changes(
        self,
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
            PostgresIndexRelationChange.from_dict(
                {"action": RelationChangeAction.drop, "context": index}
            )
            for index in existing_indexes.difference(new_indexes)
        )
        create_changes = set(
            PostgresIndexRelationChange.from_dict(
                {"action": RelationChangeAction.create, "context": index}
            )
            for index in new_indexes.difference(existing_indexes)
        )
        return set().union(drop_changes, create_changes)
