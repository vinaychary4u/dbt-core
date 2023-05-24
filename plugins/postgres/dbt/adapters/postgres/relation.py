from dataclasses import dataclass
from typing import Dict, List, Union, Optional

import agate

from dbt.adapters.base.relation import BaseRelation
from dbt.contracts.graph.model_config import NodeConfig
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.index import PostgresIndexConfig


@dataclass(frozen=True, eq=False, repr=False)
class PostgresRelation(BaseRelation):

    IndexUpdates = Dict[str, Union[str, PostgresIndexConfig]]

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

    def get_index_updates(
        self, indexes: agate.Table, config: NodeConfig
    ) -> List[Optional[IndexUpdates]]:
        """
        Get the index updates that will occur as a result of a new run

        There are four scenarios:

        1. Indexes are equal > don't return these
        2. Index is new > create these
        3. Index is old > drop these
        4. Indexes are not equal > drop old, create new

        Returns: a list of index updates in the form {"action": "drop/create", "context": <index>}

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
        },
        """
        # the columns show up as a comma-separated list in the query from postgres
        # we don't want to put this in `PostgresIndexConfig as a post_init because we don't want users
        # to be able to submit a string of columns
        indexes = [dict(index) for index in indexes.rows]
        for index in indexes:
            index["columns"] = index["columns"].split(",")

        existing_indexes = set(PostgresIndexConfig.parse(index) for index in indexes)
        new_indexes = set(PostgresIndexConfig.parse(index) for index in config.get("indexes", []))

        drop_indexes = existing_indexes.difference(new_indexes)
        create_indexes = new_indexes.difference(existing_indexes)

        drop_updates = [{"action": "drop", "context": index} for index in drop_indexes]
        create_updates = [{"action": "create", "context": index} for index in create_indexes]
        return drop_updates + create_updates
