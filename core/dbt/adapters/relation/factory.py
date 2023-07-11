from dataclasses import replace
from typing import Dict, Optional, Set, Type

from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName, RelationType

from dbt.adapters.relation import models


class RelationFactory:
    """
    Unlike other classes that get used by adapters, this class is not intended to be subclassed. Instead,
    an instance should be taken from it that takes in all the required configuration (or defaults to
    what is here).
    """

    # this configuration should never change
    backup_suffix: str = "__dbt_backup"
    intermediate_suffix: str = "__dbt_tmp"

    def __init__(
        self,
        relation_models: Dict[RelationType, Type[models.Relation]],
        relation_changesets: Optional[Dict[RelationType, Type[models.RelationChangeset]]] = None,
        relation_can_be_renamed: Optional[Set[RelationType]] = None,
        render_policy: models.RenderPolicy = models.RenderPolicy(),
    ):
        self.relation_models = relation_models
        self.relation_changesets = relation_changesets or {}
        self.relation_can_be_renamed = relation_can_be_renamed or set()
        self.render_policy = render_policy

    def make_from_model_node(self, model_node: ModelNode) -> Optional[models.Relation]:
        relation_type = RelationType(model_node.config.materialized)
        if parser := self._get_parser(relation_type):
            relation = parser.from_model_node(model_node)
            assert isinstance(relation, models.Relation)  # mypy
            return relation
        return None

    def make_from_describe_relation_results(
        self,
        describe_relation_results: models.DescribeRelationResults,
        relation_type: RelationType,
    ) -> Optional[models.Relation]:
        if parser := self._get_parser(relation_type):
            relation = parser.from_describe_relation_results(describe_relation_results)
            assert isinstance(relation, models.Relation)  # mypy
            return relation
        return None

    def make_stub(
        self,
        name: str,
        schema_name: str,
        database_name: str,
        relation_type: RelationType,
    ) -> models.RelationStub:
        relation_stub = models.RelationStub.from_dict(
            {
                "name": name,
                "schema": {
                    "name": schema_name,
                    "database": {
                        "name": database_name,
                        "render": self.render_policy,
                    },
                    "render": self.render_policy,
                },
                "render": self.render_policy,
                "type": relation_type,
                "can_be_renamed": relation_type in self.relation_can_be_renamed,
            }
        )
        return relation_stub

    def make_backup_stub(self, existing_relation: models.Relation) -> models.RelationStub:
        backup_name = self.render_policy.part(
            ComponentName.Identifier, f"{existing_relation.name}{self.backup_suffix}"
        )
        assert isinstance(backup_name, str)  # mypy
        return self.make_stub(
            name=backup_name,
            schema_name=existing_relation.schema_name,
            database_name=existing_relation.database_name,
            relation_type=existing_relation.type,
        )

    def make_intermediate(self, target_relation: models.Relation) -> models.Relation:
        intermediate_name = self.render_policy.part(
            ComponentName.Identifier, f"{target_relation.name}{self.intermediate_suffix}"
        )
        return replace(target_relation, name=intermediate_name)

    def make_changeset(
        self, existing_relation: models.Relation, target_relation: models.Relation
    ) -> Optional[models.RelationChangeset]:
        if changeset := self._get_changeset(existing_relation.type):
            return changeset.from_relations(existing_relation, target_relation)
        return None

    def _get_parser(self, relation_type: RelationType) -> Optional[Type[models.Relation]]:
        return self.relation_models.get(relation_type)

    def _get_changeset(
        self, relation_type: RelationType
    ) -> Optional[Type[models.RelationChangeset]]:
        return self.relation_changesets.get(relation_type)
