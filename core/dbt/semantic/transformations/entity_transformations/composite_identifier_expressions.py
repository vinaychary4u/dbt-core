from dbt.contracts.graph.nodes import Entity
from abc import ABC


class CompositeIdentifierExpressionRule(ABC):
    """Transform composite sub-identifiers for convenience.
    If a sub-identifier has no expression, check if an identifier exists
    with the same name and use that identifier's expression if it has one.
    """

    @staticmethod
    def _transform_entity(entity: Entity) -> Entity:  # noqa: D
        for identifier in entity.identifiers:
            if identifier.identifiers is None or len(identifier.identifiers) == 0:
                continue

            for sub_identifier in identifier.identifiers:
                if sub_identifier.name or sub_identifier.expr:
                    continue

                for identifier in entity.identifiers:
                    if sub_identifier.ref == identifier.name:
                        sub_identifier.ref = None
                        sub_identifier.name = identifier.name
                        sub_identifier.expr = identifier.expr
                        break

        return entity
