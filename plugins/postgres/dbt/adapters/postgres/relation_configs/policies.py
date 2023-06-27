from dataclasses import dataclass

from dbt.adapters.base.relation import Policy


QUOTE_CHAR = '"'
MAX_CHARACTERS_IN_IDENTIFIER = 63


class PostgresIncludePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class PostgresQuotePolicy(Policy):
    database: bool = True
    schema: bool = True
    identifier: bool = True
