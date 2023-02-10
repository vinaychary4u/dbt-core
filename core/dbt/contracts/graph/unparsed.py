from __future__ import annotations
import re

from dbt import deprecations
from dbt.node_types import NodeType
from dbt.contracts.util import (
    AdditionalPropertiesMixin,
    Mergeable,
    Replaceable,
    rename_metric_attr,
)

# trigger the PathEncoder
import dbt.helper_types  # noqa:F401
from dbt.exceptions import CompilationError, ParsingError

from dbt.dataclass_schema import dbtClassMixin, StrEnum, ExtensibleDbtClassMixin, ValidationError

# Semantic Classes
from dbt.semantic.references import (
    DimensionReference, 
    TimeDimensionReference,
    MeasureReference,
    IdentifierReference,
    CompositeSubIdentifierReference,
    LinkableElementReference
)
from dbt.semantic.aggregation_types import AggregationType
from dbt.semantic.time import TimeGranularity

from dataclasses import dataclass, field
from datetime import timedelta
from pathlib import Path
from typing import Optional, List, Union, Dict, Any, Sequence


@dataclass
class UnparsedBaseNode(dbtClassMixin, Replaceable):
    package_name: str
    path: str
    original_file_path: str

    @property
    def file_id(self):
        return f"{self.package_name}://{self.original_file_path}"


@dataclass
class HasCode(dbtClassMixin):
    raw_code: str
    language: str

    @property
    def empty(self):
        return not self.raw_code.strip()


@dataclass
class UnparsedMacro(UnparsedBaseNode, HasCode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Macro]})


@dataclass
class UnparsedGenericTest(UnparsedBaseNode, HasCode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Macro]})


@dataclass
class UnparsedNode(UnparsedBaseNode, HasCode):
    name: str
    resource_type: NodeType = field(
        metadata={
            "restrict": [
                NodeType.Model,
                NodeType.Analysis,
                NodeType.Test,
                NodeType.Snapshot,
                NodeType.Operation,
                NodeType.Seed,
                NodeType.RPCCall,
                NodeType.SqlOperation,
            ]
        }
    )

    @property
    def search_name(self):
        return self.name


@dataclass
class UnparsedRunHook(UnparsedNode):
    resource_type: NodeType = field(metadata={"restrict": [NodeType.Operation]})
    index: Optional[int] = None


@dataclass
class Docs(dbtClassMixin, Replaceable):
    show: bool = True
    node_color: Optional[str] = None


@dataclass
class HasDocs(AdditionalPropertiesMixin, ExtensibleDbtClassMixin, Replaceable):
    name: str
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    data_type: Optional[str] = None
    docs: Docs = field(default_factory=Docs)
    _extra: Dict[str, Any] = field(default_factory=dict)


TestDef = Union[Dict[str, Any], str]


@dataclass
class HasTests(HasDocs):
    tests: Optional[List[TestDef]] = None

    def __post_init__(self):
        if self.tests is None:
            self.tests = []


@dataclass
class UnparsedColumn(HasTests):
    quote: Optional[bool] = None
    tags: List[str] = field(default_factory=list)


@dataclass
class HasColumnDocs(dbtClassMixin, Replaceable):
    columns: Sequence[HasDocs] = field(default_factory=list)


@dataclass
class HasColumnTests(HasColumnDocs):
    columns: Sequence[UnparsedColumn] = field(default_factory=list)


@dataclass
class HasYamlMetadata(dbtClassMixin):
    original_file_path: str
    yaml_key: str
    package_name: str

    @property
    def file_id(self):
        return f"{self.package_name}://{self.original_file_path}"


@dataclass
class HasConfig:
    config: Dict[str, Any] = field(default_factory=dict)


@dataclass
class UnparsedAnalysisUpdate(HasConfig, HasColumnDocs, HasDocs, HasYamlMetadata):
    pass


@dataclass
class UnparsedNodeUpdate(HasConfig, HasColumnTests, HasTests, HasYamlMetadata):
    quote_columns: Optional[bool] = None


@dataclass
class MacroArgument(dbtClassMixin):
    name: str
    type: Optional[str] = None
    description: str = ""


@dataclass
class UnparsedMacroUpdate(HasConfig, HasDocs, HasYamlMetadata):
    arguments: List[MacroArgument] = field(default_factory=list)


class TimePeriod(StrEnum):
    minute = "minute"
    hour = "hour"
    day = "day"

    def plural(self) -> str:
        return str(self) + "s"


@dataclass
class Time(dbtClassMixin, Mergeable):
    count: Optional[int] = None
    period: Optional[TimePeriod] = None

    def exceeded(self, actual_age: float) -> bool:
        if self.period is None or self.count is None:
            return False
        kwargs: Dict[str, int] = {self.period.plural(): self.count}
        difference = timedelta(**kwargs).total_seconds()
        return actual_age > difference

    def __bool__(self):
        return self.count is not None and self.period is not None


@dataclass
class FreshnessThreshold(dbtClassMixin, Mergeable):
    warn_after: Optional[Time] = field(default_factory=Time)
    error_after: Optional[Time] = field(default_factory=Time)
    filter: Optional[str] = None

    def status(self, age: float) -> "dbt.contracts.results.FreshnessStatus":
        from dbt.contracts.results import FreshnessStatus

        if self.error_after and self.error_after.exceeded(age):
            return FreshnessStatus.Error
        elif self.warn_after and self.warn_after.exceeded(age):
            return FreshnessStatus.Warn
        else:
            return FreshnessStatus.Pass

    def __bool__(self):
        return bool(self.warn_after) or bool(self.error_after)


@dataclass
class AdditionalPropertiesAllowed(AdditionalPropertiesMixin, ExtensibleDbtClassMixin):
    _extra: Dict[str, Any] = field(default_factory=dict)


@dataclass
class ExternalPartition(AdditionalPropertiesAllowed, Replaceable):
    name: str = ""
    description: str = ""
    data_type: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self):
        if self.name == "" or self.data_type == "":
            raise CompilationError("External partition columns must have names and data types")


@dataclass
class ExternalTable(AdditionalPropertiesAllowed, Mergeable):
    location: Optional[str] = None
    file_format: Optional[str] = None
    row_format: Optional[str] = None
    tbl_properties: Optional[str] = None
    partitions: Optional[Union[List[str], List[ExternalPartition]]] = None

    def __bool__(self):
        return self.location is not None


@dataclass
class Quoting(dbtClassMixin, Mergeable):
    database: Optional[bool] = None
    schema: Optional[bool] = None
    identifier: Optional[bool] = None
    column: Optional[bool] = None


@dataclass
class UnparsedSourceTableDefinition(HasColumnTests, HasTests):
    config: Dict[str, Any] = field(default_factory=dict)
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(default_factory=FreshnessThreshold)
    external: Optional[ExternalTable] = None
    tags: List[str] = field(default_factory=list)

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        if "freshness" not in dct and self.freshness is None:
            dct["freshness"] = None
        return dct


@dataclass
class UnparsedSourceDefinition(dbtClassMixin, Replaceable):
    name: str
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: str = ""
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(default_factory=FreshnessThreshold)
    loaded_at_field: Optional[str] = None
    tables: List[UnparsedSourceTableDefinition] = field(default_factory=list)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    @property
    def yaml_key(self) -> "str":
        return "sources"

    def __post_serialize__(self, dct):
        dct = super().__post_serialize__(dct)
        if "freshness" not in dct and self.freshness is None:
            dct["freshness"] = None
        return dct


@dataclass
class SourceTablePatch(dbtClassMixin):
    name: str
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    data_type: Optional[str] = None
    docs: Optional[Docs] = None
    loaded_at_field: Optional[str] = None
    identifier: Optional[str] = None
    quoting: Quoting = field(default_factory=Quoting)
    freshness: Optional[FreshnessThreshold] = field(default_factory=FreshnessThreshold)
    external: Optional[ExternalTable] = None
    tags: Optional[List[str]] = None
    tests: Optional[List[TestDef]] = None
    columns: Optional[Sequence[UnparsedColumn]] = None

    def to_patch_dict(self) -> Dict[str, Any]:
        dct = self.to_dict(omit_none=True)
        remove_keys = "name"
        for key in remove_keys:
            if key in dct:
                del dct[key]

        if self.freshness is None:
            dct["freshness"] = None

        return dct


@dataclass
class SourcePatch(dbtClassMixin, Replaceable):
    name: str = field(
        metadata=dict(description="The name of the source to override"),
    )
    overrides: str = field(
        metadata=dict(description="The package of the source to override"),
    )
    path: Path = field(
        metadata=dict(description="The path to the patch-defining yml file"),
    )
    config: Dict[str, Any] = field(default_factory=dict)
    description: Optional[str] = None
    meta: Optional[Dict[str, Any]] = None
    database: Optional[str] = None
    schema: Optional[str] = None
    loader: Optional[str] = None
    quoting: Optional[Quoting] = None
    freshness: Optional[Optional[FreshnessThreshold]] = field(default_factory=FreshnessThreshold)
    loaded_at_field: Optional[str] = None
    tables: Optional[List[SourceTablePatch]] = None
    tags: Optional[List[str]] = None

    def to_patch_dict(self) -> Dict[str, Any]:
        dct = self.to_dict(omit_none=True)
        remove_keys = ("name", "overrides", "tables", "path")
        for key in remove_keys:
            if key in dct:
                del dct[key]

        if self.freshness is None:
            dct["freshness"] = None

        return dct

    def get_table_named(self, name: str) -> Optional[SourceTablePatch]:
        if self.tables is not None:
            for table in self.tables:
                if table.name == name:
                    return table
        return None


@dataclass
class UnparsedDocumentation(dbtClassMixin, Replaceable):
    package_name: str
    path: str
    original_file_path: str

    @property
    def file_id(self):
        return f"{self.package_name}://{self.original_file_path}"

    @property
    def resource_type(self):
        return NodeType.Documentation


@dataclass
class UnparsedDocumentationFile(UnparsedDocumentation):
    file_contents: str


# can't use total_ordering decorator here, as str provides an ordering already
# and it's not the one we want.
class Maturity(StrEnum):
    low = "low"
    medium = "medium"
    high = "high"

    def __lt__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        order = (Maturity.low, Maturity.medium, Maturity.high)
        return order.index(self) < order.index(other)

    def __gt__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        return self != other and not (self < other)

    def __ge__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        return self == other or not (self < other)

    def __le__(self, other):
        if not isinstance(other, Maturity):
            return NotImplemented
        return self == other or self < other


class ExposureType(StrEnum):
    Dashboard = "dashboard"
    Notebook = "notebook"
    Analysis = "analysis"
    ML = "ml"
    Application = "application"


class MaturityType(StrEnum):
    Low = "low"
    Medium = "medium"
    High = "high"


@dataclass
class ExposureOwner(dbtClassMixin, Replaceable):
    email: str
    name: Optional[str] = None


@dataclass
class UnparsedExposure(dbtClassMixin, Replaceable):
    name: str
    type: ExposureType
    owner: ExposureOwner
    description: str = ""
    label: Optional[str] = None
    maturity: Optional[MaturityType] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    url: Optional[str] = None
    depends_on: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    @classmethod
    def validate(cls, data):
        super(UnparsedExposure, cls).validate(data)
        if "name" in data:
            # name can only contain alphanumeric chars and underscores
            if not (re.match(r"[\w-]+$", data["name"])):
                deprecations.warn("exposure-name", exposure=data["name"])

#########################
## SEMANTIC LAYER CLASSES
#########################


#########################
## SL: Identifier Classes
#########################


class EntityIdentifierType(StrEnum):
    """Defines uniqueness and the extent to which an identifier represents the common entity for a data source"""

    FOREIGN = "foreign"
    NATURAL = "natural"
    PRIMARY = "primary"
    UNIQUE = "unique"


@dataclass
class EntityCompositeSubIdentifier(dbtClassMixin):
    """CompositeSubIdentifiers either describe or reference the identifiers that comprise a composite identifier"""

    name: Optional[str] = None
    expr: Optional[str] = None
    ref: Optional[str] = None

    @property
    def reference(self) -> CompositeSubIdentifierReference:  # noqa: D
        assert self.name, f"The element name should have been set during model transformation. Got {self}"
        return CompositeSubIdentifierReference(element_name=self.name)


@dataclass
class EntityIdentifier(dbtClassMixin, Mergeable):
    """Describes a identifier"""

    name: str
    type: EntityIdentifierType
    description: str = ""
    role: Optional[str] = None
    entity: Optional[str] = None
    identifiers: List[EntityCompositeSubIdentifier] = field(default_factory=list)
    expression: Optional[str] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    # Moved validation down to entity level

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        return False

    @property
    def is_composite(self) -> bool:  # noqa: D
        return self.identifiers is not None and len(self.identifiers) > 0

    @property
    def reference(self) -> IdentifierReference:  # noqa: D
        return IdentifierReference(element_name=self.name)

    @property
    def is_linkable_identifier_type(self) -> bool:
        """Indicates whether or not this identifier can be used as a linkable identifier type for joins
        That is, can you use the identifier as a linkable element in multi-hop dundered syntax. For example,
        the country dimension in the listings data source can be linked via listing__country, because listing
        is the primary key.
        At the moment, you may only request things accessible via primary, unique, or natural keys, with natural
        keys reserved for SCD Type II style data sources.
        """
        return self.type in (EntityIdentifierType.PRIMARY, EntityIdentifierType.UNIQUE, EntityIdentifierType.NATURAL)


#########################
## SL: Dimension Classes
#########################


class EntityDimensionType(StrEnum):
    categorical = "categorical"
    time = "time"

    def is_time_type(self) -> bool:
        """Checks if this type of dimension is a time type"""
        return self in [EntityDimensionType.time]


@dataclass
class EntityDimensionTypeParameters(dbtClassMixin, Mergeable):
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
    is_start: bool = False
    is_end: bool = False


@dataclass
class EntityDimension(dbtClassMixin, Mergeable):
    """Each instance of this class represents a dimension in the associated entity."""
    name: str
    type: EntityDimensionType
    type_params: Optional[EntityDimensionTypeParameters] = None
    expression: Optional[str] = None
    is_partition: bool = False
    description: str = ""
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    @property
    def is_primary_time(self) -> bool:  # noqa: D
        if self.type == EntityDimensionType.time and self.type_params is not None:
            return self.type_params.is_primary
        return False

    @property
    def reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(element_name=self.name)

    @property
    def time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D
        assert self.type == EntityDimensionType.TIME, f"Got type as {self.type} instead of {EntityDimensionType.TIME}"
        return TimeDimensionReference(element_name=self.name)

    # TODO: Get rid of this section if we can bundle validity params
    # @property
    # def validity_params(self) -> Optional[DimensionValidityParams]:
    #     """Returns the DimensionValidityParams property, if it exists.
    #     This is to avoid repeatedly checking that type params is not None before doing anything with ValidityParams
    #     """
    #     if self.type_params:
    #         return self.type_params.validity_params

    #     return None


#########################
## SL: Measure Classes
#########################


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


class MeasureAggregationState(StrEnum):
    """Represents how the measure is aggregated."""
    NON_AGGREGATED = "NON_AGGREGATED"
    PARTIAL = "PARTIAL"
    COMPLETE = "COMPLETE"

    def __repr__(self) -> str:  # noqa: D
        return f"{self.__class__.__name__}.{self.name}"


@dataclass
class EntityMeasure(dbtClassMixin, Mergeable):
    """Describes a measure"""
    name: str
    aggregation: AggregationType
    description: str = ""
    expression: Optional[str] = None
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
        return TimeDimensionReference(element_name=self.agg_time_dimension)

    @property
    def reference(self) -> MeasureReference:  # noqa: D
        return MeasureReference(element_name=self.name)


#########################
## SL: Entity Classes
#########################


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


@dataclass
class UnparsedEntity(dbtClassMixin, Replaceable):
    """This class is used for entity information"""

    name: str
    model: str
    description: str = ""
    identifiers: Optional[Sequence[EntityIdentifier]] = None
    dimensions: Optional[Sequence[EntityDimension]] = None
    measures: Optional[Sequence[EntityMeasure]] = None
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)

    # TODO: Figure out if we need this
    mutability: EntityMutability = EntityMutability(type=EntityMutabilityType.FULL_MUTATION)

    # TODO: Figure out if we need this
    # origin: DataSourceOrigin = DataSourceOrigin.SOURCE

    @classmethod
    def validate(cls, data):
        super(UnparsedEntity, cls).validate(data)
        # TODO: Replace this hacky way to verify a ref statement
        # We are using this today in order to verify that model field
        # is taking a ref input
        if "ref('" not in data['model']:
            raise ParsingError(
                f"The entity '{data['name']}' does not contain a proper ref('') in the model property."
            )
        for identifier in data['identifiers']:
            if identifier.get("entity") is None:
                if 'name' not in identifier:
                    raise ParsingError(
                        f"Failed to define identifier entity value for entity '{data['name']}' because identifier name was not defined."
                    )
                identifier["entity"]=identifier["name"]


#########################
## SL: Metric Classes
#########################


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
class MetricInputMeasure(dbtClassMixin, Mergeable):
    """Provides a pointer to a measure along with metric-specific processing directives
    If an alias is set, this will be used as the string name reference for this measure after the aggregation
    phase in the SQL plan.
    """

    name: str
    # constraint: Optional[WhereClauseConstraint]
    alias: Optional[str]=None

    @classmethod
    def _from_yaml_value(cls, input: MetricInputMeasueValue) -> MetricInputMeasure:
        """Parses a MetricInputMeasure from a string (name only) or object (struct spec) input
        For user input cases, the original YAML spec for a Metric included measure(s) specified as string names
        or lists of string names. As such, configs pre-dating the addition of this model type will only provide the
        base name for this object.
        """
        if isinstance(input, str):
            return MetricInputMeasure(name=input)
        else:
            raise ValueError(
                f"MetricInputMeasure inputs from model configs are expected to be of either type string or "
                f"object (key/value pairs), but got type {type(input)} with value: {input}"
            )

    @property
    def measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference associated with this metric input measure"""
        return MeasureReference(element_name=self.name)

    @property
    def post_aggregation_measure_reference(self) -> MeasureReference:
        """Property accessor to get the MeasureReference with the aliased name, if appropriate"""
        return MeasureReference(element_name=self.alias or self.name)


@dataclass
class MetricTimeWindow(dbtClassMixin, Mergeable):
    """Describes the window of time the metric should be accumulated over, e.g., '1 day', '2 weeks', etc"""

    count: int
    granularity: TimeGranularity

    def to_string(self) -> str:  # noqa: D
        return f"{self.count} {self.granularity.value}"

    # @staticmethod
    # def parse(window: str) -> MetricTimeWindow:
    #     """Returns window values if parsing succeeds, None otherwise
    #     Output of the form: (<time unit count>, <time granularity>, <error message>) - error message is None if window is formatted properly
    #     """
    #     parts = window.split(" ")
    #     if len(parts) != 2:
    #         raise ParsingError(
    #             f"Invalid window ({window}) in cumulative metric. Should be of the form `<count> <granularity>`, e.g., `28 days`",
    #         )

    #     granularity = parts[1]
    #     # if we switched to python 3.9 this could just be `granularity = parts[0].removesuffix('s')
    #     if granularity.endswith("s"):
    #         # months -> month
    #         granularity = granularity[:-1]
    #     if granularity not in [item.value for item in TimeGranularity]:
    #         raise ParsingError(
    #             f"Invalid time granularity {granularity} in cumulative metric window string: ({window})",
    #         )

    #     count = parts[0]
    #     if not count.isdigit():
    #         raise ParsingError(f"Invalid count ({count}) in cumulative metric window string: ({window})")

    #     return MetricTimeWindow(
    #         count=int(count),
    #         granularity=string_to_time_granularity(granularity),
    #     )


@dataclass
class MetricInput(dbtClassMixin, Mergeable):
    """Provides a pointer to a metric along with the additional properties used on that metric."""
    #TODO: Get the pointer to work

    name: str
    # constraint: Optional[WhereClauseConstraint]
    alias: Optional[str]
    offset_window: Optional[MetricTimeWindow]
    offset_to_grain: Optional[TimeGranularity]


@dataclass
class MetricTypeParams(dbtClassMixin, Mergeable):
    """Type params add additional context to certain metric types (the context depends on the metric type)"""

    measure: Optional[str] = None
    measures: Optional[List[str]]  = field(default_factory=list)
    numerator: Optional[str] = None
    denominator: Optional[str] = None
    expression: Optional[str] = None
    window: Optional[str] = None
    grain_to_date: Optional[str] = None
    metrics: Optional[List[str]] = field(default_factory=list)

    @property
    def numerator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the numerator"""
        return self.numerator.measure_reference if self.numerator else None

    @property
    def denominator_measure_reference(self) -> Optional[MeasureReference]:
        """Return the measure reference, if any, associated with the metric input measure defined as the denominator"""
        return self.denominator.measure_reference if self.denominator else None


@dataclass
class UnparsedMetric(dbtClassMixin):
    """Describes a metric"""

    name: str
    type: MetricType
    type_params: MetricTypeParams
    description: Optional[str] = None
    entity: Optional[str] = None
    # constraint: Optional[WhereClauseConstraint]
    meta: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    config: Dict[str, Any] = field(default_factory=dict)
    
    @classmethod
    def validate(cls, data):
        super(UnparsedMetric, cls).validate(data)
        if "entity" not in data:
            if data["type"] != MetricType.DERIVED:
                raise CompilationError(f"The metric {data['name']} is missing the required entity property.")
        elif "entity" in data:
            if data["type"] == MetricType.DERIVED:
                raise CompilationError(f"The metric {data['name']} is derived, which does not support entity definition.")
            # TODO: Replace this hacky way to verify an entity lookup
            # We are doing this to ensure that the entity property is using an entity
            # function and not just providing a string
            if "entity('" not in data['entity']:
                raise ParsingError(
                    f"The metric '{data['name']}' does not contain a proper entity('') reference in the entity property."
                )