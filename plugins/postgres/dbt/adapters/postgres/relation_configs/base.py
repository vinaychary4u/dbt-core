from abc import ABC
from dataclasses import dataclass

from dbt.adapters.relation_configs import RelationConfigBase

from dbt.adapters.postgres.relation_configs.policies import (
    PostgresIncludePolicy,
    PostgresQuotePolicy,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresRelationConfigBase(RelationConfigBase, ABC):
    include_policy = PostgresIncludePolicy()
    quote_policy = PostgresQuotePolicy()
