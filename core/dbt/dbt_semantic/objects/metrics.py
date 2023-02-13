from __future__ import annotations
from dbt.contracts.util import (
    Replaceable,
    Mergeable
)
from dbt.exceptions import ParsingError
from dbt.dataclass_schema import dbtClassMixin, StrEnum
from dataclasses import dataclass, field
from typing import Optional, List, Any, Union
from dbt.dbt_semantic.aggregation_types import AggregationType
from dbt.dbt_semantic.references import TimeDimensionReference, MeasureReference
from dbt.dbt_semantic.time import TimeGranularity, string_to_time_granularity


@dataclass
class MetricType(StrEnum):
    """Currently supported metric types"""

    MEASURE_PROXY = "measure_proxy"
    RATIO = "ratio"
    EXPR = "expr"
    CUMULATIVE = "cumulative"
    DERIVED = "derived"


MetricInputMeasueValue = Any

@dataclass
class MetricInputMeasure(dbtClassMixin, Replaceable):
    """Provides a pointer to a measure along with metric-specific processing directives
    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    # constraint: Optional[WhereClauseConstraint]
    alias: Optional[str]=None

    # Removed _from_yaml_value due to how dbt reads in yml

    @property
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure"""
        return MeasureReference(name=self.name)

    @property
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate"""
        return MeasureReference(name=self.alias or self.name)


@dataclass
class MetricTimeWindow(dbtClassMixin, Mergeable):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc"""

    count: int
    granularity: TimeGranularity

    def to_string(self) -> str:  # noqa: D
        return f"{self.count} {self.granularity.value}"

    @staticmethod
    def parse(window: str) -> MetricTimeWindow:
        """Returns window values if parsing succeeds, None otherwise
        Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window is formatted properly
        """
        parts = window.split(" ")
        if len(parts) != 2:
            raise ParsingError(
                f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, e.g., `28 days`",
            )

        granularity = parts[1]
        # if we switched to python 3.9 this could just be `granularity = parts[0].removesuffix('s')
        if granularity.endswith("s"):
            # months -> month
            granularity = granularity[:-1]
        if granularity not in [item.value for item in TimeGranularity]:
            raise ParsingError(
                f"Invalid time granularity {granularity} in cumulative metric window string: ({window})",
            )

        count = parts[0]
        if not count.isdigit():
            raise ParsingError(f"Invalid count ({count}) in cumulative metric window string: ({window})")

        return MetricTimeWindow(
            count=int(count),
            granularity=string_to_time_granularity(granularity),
        )


@dataclass
class MetricInput(dbtClassMixin, Mergeable):
    """Provides a pointer to a metric along with the additional properties used on that metric."""

    name: str
    # constraint: Optional[WhereClauseConstraint]
    alias: Optional[str] = None
    offset_window: Optional[MetricTimeWindow] = None
    offset_to_grain: Optional[TimeGranularity] = None


@dataclass
class UnparsedMetricTypeParams(dbtClassMixin, Mergeable):
    """Type params add additional context to certain metric types (the context depends on the metric type)"""

    #NOTE: Adding a union to allow for the class or a string. We 
    # change to prefered class in schemas.py during conversion to Metric
    measure: Optional[Union[MetricInputMeasure,str]] = None
    measures: Optional[List[Union[MetricInputMeasure,str]]]  = field(default_factory=list)
    numerator: Optional[Union[MetricInputMeasure,str]] = None
    denominator: Optional[Union[MetricInputMeasure,str]] = None
    expr: Optional[str] = None
    window: Optional[Union[MetricTimeWindow,str]] = None
    grain_to_date: Optional[TimeGranularity] = None
    metrics: Optional[List[Union[MetricInput,str]]] = field(default_factory=list)

    @property
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator"""
        return self.numerator.measure_reference if self.numerator else None

    @property
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator"""
        return self.denominator.measure_reference if self.denominator else None


@dataclass
class MetricReference(dbtClassMixin, Replaceable):
    sql: Optional[Union[str, int]]
    unique_id: Optional[str]


@dataclass
class MetricTypeParams(dbtClassMixin):
    """Type params add additional context to certain metric types (the context depends on the metric type)"""

    measure: Optional[MetricInputMeasure] = None
    measures: Optional[List[MetricInputMeasure]] = field(default_factory=list)
    numerator: Optional[MetricInputMeasure] = None
    denominator: Optional[MetricInputMeasure] = None
    expr: Optional[str] = None
    window: Optional[MetricTimeWindow] = None
    grain_to_date: Optional[TimeGranularity] = None
    metrics: Optional[List[MetricInput]] = field(default_factory=list)

    @property
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator"""
        return self.numerator.measure_reference if self.numerator else None

    @property
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator"""
        return self.denominator.measure_reference if self.denominator else None