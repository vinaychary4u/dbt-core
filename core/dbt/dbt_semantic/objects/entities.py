from __future__ import annotations
from dbt.contracts.util import (
    Mergeable
)
from dbt.dataclass_schema import dbtClassMixin, StrEnum
from dataclasses import dataclass
from typing import Optional


class EntityMutabilityType(StrEnum):
    """How data at the physical layer is expected to behave"""

    UNKNOWN = "UNKNOWN"
    IMMUTABLE = "IMMUTABLE"  # never changes
    APPEND_ONLY = "APPEND_ONLY"  # appends along an orderable column
    DS_APPEND_ONLY = "DS_APPEND_ONLY"  # appends along daily column
    FULL_MUTATION = "FULL_MUTATION"  # no guarantees, everything may change


@dataclass
class EntityMutabilityTypeParams(dbtClassMixin, Mergeable):
    """Type params add additional context to mutability"""

    min: Optional[str] = None
    max: Optional[str] = None
    update_cron: Optional[str] = None
    along: Optional[str] = None


@dataclass
class EntityMutability(dbtClassMixin):
    """Describes the mutability properties of a data source"""

    type: EntityMutabilityType
    type_params: Optional[EntityMutabilityTypeParams] = None