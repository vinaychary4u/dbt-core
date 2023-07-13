from abc import ABC
from dataclasses import dataclass
from typing import Any, Dict, Optional

from dbt.adapters.relation.factory import RelationFactory
from dbt.adapters.relation.models import Relation, RelationRef
from dbt.contracts.graph.nodes import CompiledNode

from dbt.adapters.materialization.models._materialization import (
    Materialization,
    MaterializationBuildStrategy,
    MaterializationType,
)


@dataclass
class MaterializedViewMaterialization(Materialization, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    target_relation: Relation
    existing_relation_ref: Optional[RelationRef] = None

    @property
    def build_strategy(self) -> MaterializationBuildStrategy:

        # this is a new relation, so just create it
        if self.existing_relation_ref is None:
            return MaterializationBuildStrategy.Create

        # there is an existing relation, so check if we are going to replace it before determining changes
        elif self.is_full_refresh or (
            self.target_relation.type != self.existing_relation_ref.type
        ):
            return MaterializationBuildStrategy.Replace

        # `target_relation` and `existing_relation` both exist and are the same type, so we need to determine changes
        else:
            return MaterializationBuildStrategy.Alter

    @classmethod
    def parse_node(
        cls,
        node: CompiledNode,
        relation_factory: RelationFactory,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> Dict[str, Any]:
        config_dict = super().parse_node(node, relation_factory, existing_relation_ref)
        config_dict.update({"type": MaterializationType.MaterializedView})
        return config_dict
