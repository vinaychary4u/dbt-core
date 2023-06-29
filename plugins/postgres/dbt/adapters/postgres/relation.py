from dataclasses import dataclass

from dbt.adapters.base.relation import BaseRelation
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.materialization_config import (
    PostgresIncludePolicy,
    PostgresQuotePolicy,
    MAX_CHARACTERS_IN_IDENTIFIER,
)


@dataclass(frozen=True, eq=False, repr=False)
class PostgresRelation(BaseRelation):
    include_policy = PostgresIncludePolicy()
    quote_policy = PostgresQuotePolicy()

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
