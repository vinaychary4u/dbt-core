from abc import ABC
from dbt.semantic.aggregation_properties import AggregationType
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.measures import MeasureAggregationParameters
from dbt.exceptions import DbtSemanticValidationError

MEDIAN_PERCENTILE = 0.5

class ConvertMedianToPercentile(ABC):
    """Converts any MEDIAN measures to percentile equivalent."""

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:  # noqa: D
        if entity.measures:
            for measure in entity.measures:
                if measure.agg == AggregationType.MEDIAN:
                    measure.agg = AggregationType.PERCENTILE

                    if not measure.agg_params:
                        measure.agg_params = MeasureAggregationParameters()
                    else:
                        if measure.agg_params.percentile is not None and measure.agg_params.percentile != 0.5:
                            raise DbtSemanticValidationError(
                                f"Measure '{measure.name}' uses a MEDIAN aggregation, while percentile is set to "
                                f"'{measure.agg_params.percentile}', a conflicting value. Please remove the parameter "
                                "or set to '0.5'."
                            )
                        if measure.agg_params.use_discrete_percentile:
                            raise DbtSemanticValidationError(
                                f"Measure '{measure.name}' uses a MEDIAN aggregation, while use_discrete_percentile"
                                f"is set to true. Please remove the parameter or set to False."
                            )
                    measure.agg_params.percentile = MEDIAN_PERCENTILE
                    # let's not set use_approximate_percentile to be false due to valid performance reasons
        return entity