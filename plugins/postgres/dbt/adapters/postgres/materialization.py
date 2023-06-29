from datetime import datetime
from typing import FrozenSet, Set

from dbt.adapters.base.materialization import Materialization
from dbt.adapters.materialization_config import (
    MaterializationConfig,
    RelationConfigChangeAction,
)
from dbt.contracts.relation import ComponentName, RelationType
from dbt.exceptions import DbtRuntimeError
import dbt.utils

from dbt.adapters.postgres.materialization_config import (
    PostgresIncludePolicy,
    PostgresIndexConfig,
    PostgresIndexConfigChange,
    PostgresMaterializedViewConfig,
    PostgresMaterializedViewConfigChangeset,
    PostgresQuotePolicy,
    postgres_conform_part,
)


class PostgresMaterialization(Materialization):
    materialization_configs = {RelationType.MaterializedView: PostgresMaterializedViewConfig}
    include_policy = PostgresIncludePolicy()
    quote_policy = PostgresQuotePolicy()

    @classmethod
    def materialized_view_config_changeset(
        cls,
        existing_materialized_view: PostgresMaterializedViewConfig,
        new_materialized_view: PostgresMaterializedViewConfig,
    ) -> PostgresMaterializedViewConfigChangeset:
        try:
            assert isinstance(existing_materialized_view, PostgresMaterializedViewConfig)
            assert isinstance(new_materialized_view, PostgresMaterializedViewConfig)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two materialized view configs were expected, but received:"
                f"/n    {existing_materialized_view}"
                f"/n    {new_materialized_view}"
            )

        config_changeset = PostgresMaterializedViewConfigChangeset()

        config_changeset.indexes = cls.index_config_changeset(
            existing_materialized_view.indexes, new_materialized_view.indexes
        )

        if config_changeset.is_empty and existing_materialized_view != new_materialized_view:
            # we need to force a full refresh if we didn't detect any changes but the objects are not the same
            config_changeset.force_full_refresh()

        return config_changeset

    @classmethod
    def index_config_changeset(
        cls,
        existing_indexes: FrozenSet[PostgresIndexConfig],
        new_indexes: FrozenSet[PostgresIndexConfig],
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

    @classmethod
    def generate_index_name(
        cls,
        materialization_config: MaterializationConfig,
        index_config: PostgresIndexConfig,
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
