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


class ModelReference(dbtClassMixin):
    """A reference to something in the model.
    For example, a measure instance could have a defined_from field that has a model reference to the measure / data
    source that it is supposed to reference. Added for exploratory purposes, so whether this is needed is TBD.
    """

    pass


@dataclass(frozen=True)
class EntityReference(ModelReference):
    """A reference to a entity definition in the model."""

    entity_name: str

    def __hash__(self) -> int:  # noqa: D
        return hash(self.entity_name)


@dataclass(frozen=True)
class EntityElementReference(ModelReference):
    """A reference to an element definition in a data source definition in the model.
    TODO: Fields should be *Reference objects.
    """

    entity_name: str
    name: str

    @staticmethod
    def create_from_references(  # noqa: D
        entity_reference: EntityReference, element_reference: ElementReference
    ) -> EntityElementReference:
        return EntityElementReference(
            entity_name=entity_reference.entity_name,
            name=element_reference.name,
        )

    @property
    def entity_reference(self) -> EntityReference:  # noqa: D
        return EntityReference(self.entity_name)

    def is_from(self, ref: EntityReference) -> bool:
        """Returns true if this reference is from the same data source as the supplied reference."""
        return self.entity_name == ref.entity_name


@dataclass(frozen=True)
class MetricModelReference(ModelReference):
    """A reference to a metric definition in the model."""

    metric_name: str
