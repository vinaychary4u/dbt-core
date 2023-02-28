from __future__ import annotations
from dbt.contracts.util import Mergeable
from dbt.dataclass_schema import dbtClassMixin, StrEnum
from dataclasses import dataclass, field
from typing import Optional, Dict, Any, List
from dbt.semantic.time import TimeGranularity
from dbt.semantic.references import DimensionReference, TimeDimensionReference


class DimensionType(StrEnum):
    CATEGORICAL = "categorical"
    TIME = "time"

    def is_time_type(self) -> bool:
        """Checks if this type of dimension is a time type"""
        return self in [DimensionType.time]


@dataclass
class DimensionValidityParams(dbtClassMixin, Mergeable):
    """Parameters identifying a given dimension as an identifier for validity state
    This construct is used for supporting SCD Type II tables, such as might be
    created via dbt's snapshot feature, or generated via periodic loads from external
    dimension data sources. In either of those cases, there is typically a time dimension
    associated with the SCD data source that indicates the start and end times of a
    validity window, where the dimension value is valid for any time within that range.
    """

    is_start: bool = False
    is_end: bool = False


@dataclass
class DimensionTypeParameters(dbtClassMixin, Mergeable):
    """This class contains the type parameters required for the semantic layer.
    The first iteration of this is specifically focused on time.

    Additionally we use the final two properties (start/end) for  supporting SCD
    Type II tables, such as might be created via dbt's snapshot feature, or generated
    via periodic loads from external dimension data sources. In either of those cases,
    there is typically a time dimension associated with the SCD data source that
    indicates the start and end times of a validity window, where the dimension
    value is valid for any time within that range.

    TODO: Can we abstract from params and have these be first class??"""

    is_primary: bool = False
    time_granularity: TimeGranularity = None
    validity_params: Optional[DimensionValidityParams] = None


@dataclass
class Dimension(dbtClassMixin, Mergeable):
    """Each instance of this class represents a dimension in the associated entity."""

    name: str
    type: DimensionType
    type_params: Optional[DimensionTypeParameters] = None
    expr: Optional[str] = None
    is_partition: bool = False
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        if self.type == DimensionType.TIME and self.type_params is not None:
            return self.type_params.is_primary
        return False

    @property
    def reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(name=self.name)

    @property
    def time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D
        assert (
            self.type == DimensionType.TIME
        ), f"Got type as {self.type} instead of {DimensionType.TIME}"
        return TimeDimensionReference(name=self.name)

    @property
    def validity_params(self) -> Optional[DimensionValidityParams]:
        """Returns the DimensionValidityParams property, if it exists.
        This is to avoid repeatedly checking that type params is not None before doing anything with ValidityParams
        """
        if self.type_params:
            return self.type_params.validity_params

        return None
