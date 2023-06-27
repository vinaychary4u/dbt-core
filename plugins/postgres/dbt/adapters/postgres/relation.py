from dataclasses import dataclass, field
from typing import Set, FrozenSet

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import RelationConfigChangeAction
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation_configs import (
    PostgresIndexConfig,
    PostgresIndexConfigChange,
    PostgresMaterializedViewConfig,
    PostgresMaterializedViewConfigChangeset,
    MAX_CHARACTERS_IN_IDENTIFIER,
    PostgresIncludePolicy,
    PostgresQuotePolicy,
)


@dataclass(frozen=True, eq=False, repr=False)
class PostgresRelation(BaseRelation):
    include_policy: PostgresIncludePolicy = field(default_factory=PostgresIncludePolicy)
    quote_policy: PostgresQuotePolicy = field(default_factory=PostgresQuotePolicy)

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

    @classmethod
    def materialized_view_config_changeset(
        cls,
        new_materialized_view: PostgresMaterializedViewConfig,
        existing_materialized_view: PostgresMaterializedViewConfig,
    ) -> PostgresMaterializedViewConfigChangeset:
        try:
            assert isinstance(new_materialized_view, PostgresMaterializedViewConfig)
            assert isinstance(existing_materialized_view, PostgresMaterializedViewConfig)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two materialized view configs were expected, but received:"
                f"/n    {new_materialized_view}"
                f"/n    {existing_materialized_view}"
            )

        config_changeset = PostgresMaterializedViewConfigChangeset()

        config_changeset.indexes = cls.index_config_changeset(
            new_materialized_view.indexes, existing_materialized_view.indexes
        )

        if config_changeset.is_empty and new_materialized_view != existing_materialized_view:
            # we need to force a full refresh if we didn't detect any changes but the objects are not the same
            config_changeset.force_full_refresh()

        return config_changeset

    @classmethod
    def index_config_changeset(
        cls,
        new_indexes: FrozenSet[PostgresIndexConfig],
        existing_indexes: FrozenSet[PostgresIndexConfig],
    ) -> Set[PostgresIndexConfigChange]:
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
            PostgresIndexConfigChange(action=RelationConfigChangeAction.drop, context=index)
            for index in existing_indexes.difference(new_indexes)
        )
        create_changes = set(
            PostgresIndexConfigChange(action=RelationConfigChangeAction.create, context=index)
            for index in new_indexes.difference(existing_indexes)
        )
        return set().union(drop_changes, create_changes)
