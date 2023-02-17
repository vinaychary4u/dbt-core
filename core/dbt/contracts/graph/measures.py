from __future__ import annotations
from dbt.contracts.util import (
    Replaceable,
    Mergeable
)
from dbt.dataclass_schema import dbtClassMixin
from dataclasses import dataclass, field
from typing import Optional, List, Dict, Any
from dbt.semantic.aggregation_types import AggregationType
from dbt.semantic.references import TimeDimensionReference, MeasureReference


@dataclass
class MeasureAggregationParameters(dbtClassMixin, Replaceable):
    """Describes parameters for aggregations"""
    percentile: Optional[float] = None
    use_discrete_percentile: bool = False
    use_approximate_percentile: bool = False


@dataclass
class MeasureNonAdditiveDimensionParameters(dbtClassMixin, Replaceable):
    """Describes the params for specifying non-additive dimensions in a measure.
    NOTE: Currently, only TimeDimensions are supported for this filter
    """
    name: str
    window_choice: AggregationType = AggregationType.MIN
    window_groupings: List[str] = field(default_factory=list)


@dataclass
class Measure(dbtClassMixin, Mergeable):
    """Describes a measure"""
    name: str
    agg: AggregationType
    description: str = ""
    expr: Optional[str] = None
    create_metric: Optional[bool] = None
    agg_params: Optional[MeasureAggregationParameters]=None
    non_additive_dimension: Optional[MeasureNonAdditiveDimensionParameters] = None
    # Defines the time dimension to aggregate this measure by. If not specified, it means to use the primary time
    # dimension in the data source.
    agg_time_dimension: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    @property
    def checked_agg_time_dimension(self) -> TimeDimensionReference:
        """Returns the aggregation time dimension, throwing an exception if it's not set."""
        assert self.agg_time_dimension, (
            f"Aggregation time dimension for measure {self.name} is not set! This should either be set directly on "
            f"the measure specification in the model, or else defaulted to the primary time dimension in the data "
            f"source containing the measure."
        )
        return TimeDimensionReference(name=self.agg_time_dimension)

    @property
    def reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(name=self.name)