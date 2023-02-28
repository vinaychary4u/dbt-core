from __future__ import annotations
from dbt.contracts.util import (
    Mergeable,
)
from dbt.dataclass_schema import dbtClassMixin, StrEnum
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from dbt.semantic.references import CompositeSubIdentifierReference, IdentifierReference


class IdentifierType(StrEnum):
    """Defines uniqueness and the extent to which an identifier represents the common entity for a data source"""

    FOREIGN = "foreign"
    NATURAL = "natural"
    PRIMARY = "primary"
    UNIQUE = "unique"


@dataclass
class CompositeSubIdentifier(dbtClassMixin):
    """CompositeSubIdentifiers either describe or reference the identifiers that comprise a composite identifier"""

    name: Optional[str] = None
    expr: Optional[str] = None
    ref: Optional[str] = None

    @property
    def reference(self) -> CompositeSubIdentifierReference:  # noqa: D
        assert (
            self.name
        ), f"The element name should have been set during model transformation. Got {self}"
        return CompositeSubIdentifierReference(name=self.name)


@dataclass
class Identifier(dbtClassMixin, Mergeable):
    """Describes a identifier"""

    name: str
    type: IdentifierType
    description: str = ""
    role: Optional[str] = None
    entity: Optional[str] = None
    identifiers: List[CompositeSubIdentifier] = field(default_factory=list)
    expr: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    # Moved validation down to entity level. No more default_entity_value

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        return False

    @property
    def is_composite(self) -> bool:  # noqa: D
        return self.identifiers is not None and len(self.identifiers) > 0

    @property
    def reference(self) -> IdentifierReference:  # noqa: D
        return IdentifierReference(name=self.name)

    @property
    def is_linkable_identifier_type(self) -> bool:
        """Indicates whether or not this identifier can be used as a linkable identifier type for joins
        That is, can you use the identifier as a linkable element in multi-hop dundered syntax. For example,
        the country dimension in the listings data source can be linked via listing__country, because listing
        is the primary key.
        At the moment, you may only request things accessible via primary, unique, or natural keys, with natural
        keys reserved for SCD Type II style data sources.
        """
        return self.type in (IdentifierType.PRIMARY, IdentifierType.UNIQUE, IdentifierType.NATURAL)
