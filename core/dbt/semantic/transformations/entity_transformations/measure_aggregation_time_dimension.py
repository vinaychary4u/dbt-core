from abc import ABC
from typing import Optional
from dbt.contracts.graph.nodes import Entity
from dbt.contracts.graph.dimensions import DimensionType
from dbt.semantic.references import TimeDimensionReference


class SetMeasureAggregationTimeDimension(ABC):
    """Sets the aggregation time dimension for measures to the primary time dimension if not defined."""

    @staticmethod
    def _find_primary_time_dimension(entity: Entity) -> Optional[TimeDimensionReference]:
        for dimension in entity.dimensions:
            if (
                dimension.type == DimensionType.TIME
                and dimension.type_params
                and dimension.type_params.is_primary
            ):
                return dimension.time_dimension_reference
        return None

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:  # noqa: D

        primary_time_dimension_reference = (
            SetMeasureAggregationTimeDimension._find_primary_time_dimension(entity=entity)
        )

        if primary_time_dimension_reference:
            if entity.measures:
                for measure in entity.measures:
                    if not measure.agg_time_dimension:
                        measure.agg_time_dimension = primary_time_dimension_reference.name

        return entity
