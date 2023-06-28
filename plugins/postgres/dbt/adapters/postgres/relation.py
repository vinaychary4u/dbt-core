from dataclasses import dataclass, field
from datetime import datetime
from typing import Set, FrozenSet, Dict

from dbt.adapters.base.relation import BaseRelation
from dbt.adapters.relation_configs import (
    RelationConfigChangeAction,
    RelationConfig,
    MaterializationConfig,
)
from dbt.contracts.relation import ComponentName, RelationType
from dbt.exceptions import DbtRuntimeError
import dbt.utils

from dbt.adapters.postgres.relation_configs import (
    PostgresIndexConfig,
    PostgresIndexConfigChange,
    PostgresMaterializedViewConfig,
    PostgresMaterializedViewConfigChangeset,
    PostgresIncludePolicy,
    PostgresQuotePolicy,
    postgres_conform_part,
    MAX_CHARACTERS_IN_IDENTIFIER,
)


@dataclass(frozen=True, eq=False, repr=False)
class PostgresRelation(BaseRelation):
    include_policy: PostgresIncludePolicy = field(default_factory=PostgresIncludePolicy)
    quote_policy: PostgresQuotePolicy = field(default_factory=PostgresQuotePolicy)

    @classmethod
    def relation_configs(cls) -> Dict[RelationType, RelationConfig]:
        return {RelationType.MaterializedView: PostgresMaterializedViewConfig}  # type: ignore

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

    @staticmethod
    def generate_index_name(
        materialization_config: MaterializationConfig, index_config: PostgresIndexConfig
    ) -> str:
        return dbt.utils.md5(
            "_".join(
                {
                    postgres_conform_part(
                        ComponentName.Database, materialization_config.database_name
                    ),
                    postgres_conform_part(
                        ComponentName.Schema, materialization_config.schema_name
                    ),
                    postgres_conform_part(ComponentName.Identifier, materialization_config.name),
                    *sorted(
                        postgres_conform_part(ComponentName.Identifier, column)
                        for column in index_config.column_names
                    ),
                    str(index_config.unique),
                    str(index_config.method),
                    str(datetime.utcnow().isoformat()),
                }
            )
        )
