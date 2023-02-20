from abc import ABC
from dbt.contracts.graph.nodes import Entity
from dbt.semantic.aggregation_properties import AggregationType
from dbt.exceptions import DbtSemanticValidationError

class BooleanMeasureAggregation(ABC):
    """Converts the expression used in boolean measures so that it can be aggregated."""

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:  # noqa: D
        if entity.measures:
            for measure in entity.measures:
                if measure.agg == AggregationType.SUM_BOOLEAN:
                    if measure.expr:
                        measure.expr = f"case when {measure.expr} then 1 else 0 end"
                    else:
                        measure.expr = f"case when {measure.name} then 1 else 0 end"

                    measure.agg = AggregationType.SUM

        return entity