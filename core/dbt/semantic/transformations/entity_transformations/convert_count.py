from abc import ABC
from dbt.semantic.aggregation_types import AggregationType
from dbt.contracts.graph.nodes import Entity

ONE = "1"

class ConvertCountToSum(ABC):
    """Converts any COUNT measures to SUM equivalent."""

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:  # noqa: D
        if entity.measures:
            for measure in entity.measures:
                if measure.agg == AggregationType.COUNT:
                    #NOTE: Removed if expr none error because dbt metric design encourages count on 
                    # columns, not requiring an expression. This makes it easier for users.
                    if measure.expr is None:
                        measure.expr = f"case when {measure.name} is not null then 1 else 0 end"
                    elif measure.expr != ONE:
                        # Just leave it as SUM(1) if we want to count all
                        measure.expr = f"case when {measure.expr} is not null then 1 else 0 end"
                    measure.agg = AggregationType.SUM
        return entity