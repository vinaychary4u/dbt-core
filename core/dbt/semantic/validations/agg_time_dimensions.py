from typing import List

from dbt.semantic.references import EntityElementReference, TimeDimensionReference
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import DimensionType
from dbt.contracts.graph.manifest import UserConfiguredModel
from dbt.semantic.validations.validator_helpers import (
    EntityElementContext,
    EntityElementType,
    ModelValidationRule,
    ValidationIssueType,
    ValidationError,
)


class AggregationTimeDimensionRule(ModelValidationRule):
    """Checks that the aggregation time dimension for a measure points to a valid time dimension in the entity."""

    @staticmethod
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []
        for entity in model.entities:
            issues.extend(AggregationTimeDimensionRule._validate_entity(entity))

        return issues

    @staticmethod
    def _time_dimension_in_model(time_dimension_reference: TimeDimensionReference, entity: Entity) -> bool:
        for dimension in entity.dimensions:
            if dimension.type == DimensionType.TIME and dimension.name == time_dimension_reference.name:
                return True
        return False

    @staticmethod
    def _validate_entity(entity: Entity) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for measure in entity.measures:
            measure_context = EntityElementContext(
                entity_element=EntityElementReference(
                    entity_name=entity.name, name=measure.name
                ),
                element_type=EntityElementType.MEASURE,
            )
            agg_time_dimension_reference = measure.checked_agg_time_dimension
            if not AggregationTimeDimensionRule._time_dimension_in_model(
                time_dimension_reference=agg_time_dimension_reference, entity=entity
            ):
                issues.append(
                    ValidationError(
                        context=measure_context,
                        message=f"In entity '{entity.name}', measure '{measure.name}' has the aggregation "
                        f"time dimension set to '{agg_time_dimension_reference.name}', "
                        f"which is not a valid time dimension in the entity",
                    )
                )

        return issues