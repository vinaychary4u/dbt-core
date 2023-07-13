from typing import Dict, Optional, Type

from dbt.adapters.materialization import models
from dbt.adapters.relation import RelationFactory
from dbt.adapters.relation.models import RelationRef
from dbt.contracts.graph.nodes import ParsedNode
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.materialization.models import (
    Materialization,
    MaterializationType,
    MaterializedViewMaterialization,
)


class MaterializationFactory:
    def __init__(
        self,
        **kwargs,
    ):
        # the `StrEnum` will generally be `MaterializationType`, however this allows for extending that Enum
        self.relation_factory: RelationFactory = kwargs.get("relation_factory", RelationFactory())
        self.materialization_types: Type[StrEnum] = kwargs.get(
            "materialization_types", MaterializationType
        )
        self.materialization_models: Dict[StrEnum, Type[models.Materialization]] = kwargs.get(
            "materialization_models",
            {
                MaterializationType.MaterializedView: MaterializedViewMaterialization,
            },
        )

        try:
            assert {self.materialization_models.keys}.issubset({self.materialization_types})
        except AssertionError:
            unmapped_models = {self.materialization_models.keys}.difference(
                {self.materialization_types}
            )
            raise DbtRuntimeError(
                f"Received models for {', '.join(str(model) for model in unmapped_models)} "
                f"but these materialization types are not registered on this factory.\n"
                f"    registered materialization types: {', '.join(self.materialization_types)}\n"
            )

    def make_from_node(
        self,
        node: ParsedNode,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> models.Materialization:
        materialization_type = self._get_materialization_type(node.config.materialized)
        materialization = self._get_materialization_model(materialization_type)
        return materialization.from_node(
            node=node,
            relation_factory=self.relation_factory,
            existing_relation_ref=existing_relation_ref,
        )

    def _get_materialization_type(self, materialization_type: str) -> StrEnum:
        try:
            return self.materialization_types(materialization_type)
        except ValueError:
            raise DbtRuntimeError(
                f"This factory does not recognize this materialization type.\n"
                f"    received: {materialization_type}\n"
                f"    options: {', '.join(t for t in self.materialization_types)}\n"
            )

    def _get_materialization_model(self, materialization_type: StrEnum) -> Type[Materialization]:
        if materialization := self.materialization_models.get(materialization_type):
            return materialization
        raise DbtRuntimeError(
            f"This factory does not have a materialization for this type.\n"
            f"    received: {materialization_type}\n"
            f"    options: {', '.join(t for t in self.materialization_models.keys())}\n"
        )
