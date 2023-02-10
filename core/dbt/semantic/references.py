from __future__ import annotations

from dataclasses import dataclass

from dbt.dataclass_schema import dbtClassMixin


@dataclass(frozen=True)
class ElementReference(dbtClassMixin):
    """Used when we need to refer to a dimension, measure, identifier, but other attributes are unknown."""

    name: str


@dataclass(frozen=True)
class LinkableElementReference(ElementReference):
    """Used when we need to refer to a dimension or identifier, but other attributes are unknown."""

    pass


@dataclass(frozen=True)
class MeasureReference(ElementReference):
    """Used when we need to refer to a measure (separate from LinkableElementReference because measures aren't linkable"""

    pass


@dataclass(frozen=True)
class DimensionReference(LinkableElementReference):  # noqa: D
    pass

    @property
    def time_dimension_reference(self) -> TimeDimensionReference:  # noqa: D
        return TimeDimensionReference(name=self.name)


@dataclass(frozen=True)
class TimeDimensionReference(DimensionReference):  # noqa: D
    pass

    def dimension_reference(self) -> DimensionReference:  # noqa: D
        return DimensionReference(name=self.name)


@dataclass(frozen=True)
class IdentifierReference(LinkableElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class CompositeSubIdentifierReference(ElementReference):  # noqa: D
    pass


@dataclass(frozen=True)
class MetricReference(ElementReference):  # noqa: D
    pass
