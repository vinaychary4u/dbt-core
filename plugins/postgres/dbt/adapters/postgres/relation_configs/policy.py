from dataclasses import dataclass
from functools import partial

from dbt.adapters.relation_configs import (
    IncludePolicy,
    QuotePolicy,
    conform_part,
    render_part,
    render,
)


MAX_CHARACTERS_IN_IDENTIFIER = 63


class PostgresIncludePolicy(IncludePolicy):
    database: bool = True
    schema: bool = True
    identifier: bool = True


@dataclass
class PostgresQuotePolicy(QuotePolicy):
    database: bool = True
    schema: bool = True
    identifier: bool = True

    @property
    def quote_character(self) -> str:
        """This is property to appeal to the `Policy` serialization."""
        return '"'


postgres_conform_part = partial(
    conform_part,
    quote_policy=PostgresQuotePolicy(),
)
postgres_render_part = partial(
    render_part,
    quote_policy=PostgresQuotePolicy(),
    include_policy=PostgresIncludePolicy(),
)
postgres_render = partial(
    render,
    quote_policy=PostgresQuotePolicy(),
    include_policy=PostgresIncludePolicy(),
    delimiter=".",
)
