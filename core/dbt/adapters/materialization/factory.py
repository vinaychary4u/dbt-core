from typing import Dict, Optional, Type

from dbt.adapters.materialization import models
from dbt.adapters.relation import RelationFactory
from dbt.adapters.relation.models import RelationRef
from dbt.contracts.graph.nodes import CompiledNode
from dbt.exceptions import DbtRuntimeError


class MaterializationFactory:
    def __init__(
        self,
        relation_factory: RelationFactory,
        materialization_map: Dict[models.MaterializationType, Type[models.Materialization]],
    ):
        self.relation_factory = relation_factory
        self.materialization_map = materialization_map

    def make_from_node(
        self,
        node: CompiledNode,
        materialization_type: models.MaterializationType,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> models.Materialization:
        materialization = self._get_materialization_class(materialization_type)
        return materialization.from_node(
            node=node,
            relation_factory=self.relation_factory,
            existing_relation_ref=existing_relation_ref,
        )

    def _get_materialization_class(
        self, materialization_type: models.MaterializationType
    ) -> Type[models.Materialization]:
        if materialization := self.materialization_map.get(materialization_type):
            return materialization
        raise DbtRuntimeError(
            f"This factory does not have a materialization for this type.\n"
            f"    received: {materialization_type}\n"
            f"    options: {', '.join(t for t in self.materialization_map.keys())}\n"
        )
