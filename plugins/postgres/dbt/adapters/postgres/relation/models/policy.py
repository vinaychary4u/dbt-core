from dataclasses import dataclass

from dbt.adapters.relation.models import IncludePolicy, QuotePolicy, RenderPolicy


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


PostgresRenderPolicy = RenderPolicy(
    quote_policy=PostgresQuotePolicy(),
    include_policy=PostgresIncludePolicy(),
    quote_character='"',
    delimiter=".",
)
