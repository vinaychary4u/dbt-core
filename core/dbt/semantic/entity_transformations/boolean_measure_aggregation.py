from abc import ABC
from dbt.contracts.graph.nodes import Entity
from dbt.semantic.aggregation_types import AggregationType
from dbt.exceptions import DbtSemanticValidationError

class BooleanMeasureAggregation(ABC):
    """Converts the expression used in boolean measures so that it can be aggregated."""

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:  # noqa: D
        if entity.measures:
            for measure in entity.measures:
                if measure.aggregation == AggregationType.BOOLEAN:
                    raise DbtSemanticValidationError(
                        f"In data source {entity.name}, measure `{measure.reference.name}` "
                        f"is configured as aggregation type `boolean`, which has been deprecated. Please use "
                        f"`sum_boolean` instead."
                    )
                if measure.aggregation == AggregationType.BOOLEAN or measure.aggregation == AggregationType.SUM_BOOLEAN:
                    if measure.expression:
                        measure.expression = f"CASE WHEN {measure.expression} THEN 1 ELSE 0 END"
                    else:
                        measure.expression = f"CASE WHEN {measure.name} THEN 1 ELSE 0 END"

                    measure.aggregation = AggregationType.SUM

        return entity