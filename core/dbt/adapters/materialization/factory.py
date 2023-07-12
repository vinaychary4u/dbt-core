from typing import Dict, Optional, Type

from dbt.adapters.materialization import models
from dbt.adapters.relation import RelationFactory
from dbt.adapters.relation.models import RelationRef


class MaterializationFactory:
    def __init__(
        self,
        relation_factory: Optional[RelationFactory] = None,
        materialization_map: Optional[
            Dict[models.MaterializationType, Type[models.Materialization]]
        ] = None,
    ):
        self.relation_factory = relation_factory or RelationFactory()
        self.materialization_map = materialization_map or {
            models.MaterializationType.MaterializedView: models.MaterializedViewMaterialization
        }

    def make_from_runtime_config(
        self,
        runtime_config,
        materialization_type: models.MaterializationType,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> Optional[models.Materialization]:
        if parser := self._get_parser(materialization_type):
            assert self.relation_factory is not None  # mypy
            return parser.from_runtime_config(
                runtime_config=runtime_config,
                relation_factory=self.relation_factory,
                existing_relation_ref=existing_relation_ref,
            )
        return None

    def _get_parser(
        self, materialization_type: models.MaterializationType
    ) -> Optional[Type[models.Materialization]]:
        return self.materialization_map.get(materialization_type)
