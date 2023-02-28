from dbt.contracts.graph.nodes import Entity
from abc import ABC


class LowerCaseNames(ABC):
    """Lowercases the names of both top level objects and entity elements"""

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:
        """Lowercases the names of data source elements."""
        entity.name = entity.name.lower()
        if entity.measures:
            for measure in entity.measures:
                measure.name = measure.name.lower()
        if entity.identifiers:
            for identifier in entity.identifiers:
                identifier.name = identifier.name.lower()
        if entity.dimensions:
            for dimension in entity.dimensions:
                dimension.name = dimension.name.lower()
        return entity
