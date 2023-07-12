from dataclasses import replace
from typing import Dict, Optional, Set, Type

from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName, RelationType
from dbt.exceptions import DbtRuntimeError

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
        relation_models: Optional[Dict[RelationType, Type[models.Relation]]] = None,
        relation_changesets: Optional[Dict[RelationType, Type[models.RelationChangeset]]] = None,
        relation_can_be_renamed: Optional[Set[RelationType]] = None,
        render_policy: Optional[models.RenderPolicy] = None,
    ):
        if relation_models:
            self.relation_models = relation_models
        else:
            self.relation_models = {RelationType.MaterializedView: models.MaterializedViewRelation}

        if relation_changesets:
            self.relation_changesets = relation_changesets
        else:
            self.relation_changesets = {
                RelationType.MaterializedView: models.MaterializedViewRelationChangeset
            }

        self.relation_can_be_renamed = relation_can_be_renamed or set()
        self.render_policy = render_policy if render_policy else models.RenderPolicy()

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

    def make_ref(
        self,
        name: str,
        schema_name: str,
        database_name: str,
        relation_type: RelationType,
    ) -> models.RelationRef:
        relation_ref = models.RelationRef.from_dict(
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
        return relation_ref

    def make_backup_ref(self, existing_relation: models.Relation) -> Optional[models.RelationRef]:
        if existing_relation.can_be_renamed:
            backup_name = self.render_policy.part(
                ComponentName.Identifier, f"{existing_relation.name}{self.backup_suffix}"
            )
            assert isinstance(backup_name, str)  # mypy
            backup_ref = self.make_ref(
                name=backup_name,
                schema_name=existing_relation.schema_name,
                database_name=existing_relation.database_name,
                relation_type=existing_relation.type,
            )
        else:
            raise DbtRuntimeError(
                f"This relation cannot be renamed, hence it cannot be backed up: \n"
                f"    path: {existing_relation.fully_qualified_path}\n"
                f"    type: {existing_relation.type}\n"
            )
        return backup_ref

    def make_intermediate(self, target_relation: models.Relation) -> Optional[models.Relation]:
        if target_relation.can_be_renamed:
            intermediate_name = self.render_policy.part(
                ComponentName.Identifier, f"{target_relation.name}{self.intermediate_suffix}"
            )
            intermediate = replace(target_relation, name=intermediate_name)
        else:
            raise DbtRuntimeError(
                f"This relation cannot be renamed, hence it cannot be staged: \n"
                f"    path: {target_relation.fully_qualified_path}\n"
                f"    type: {target_relation.type}\n"
            )
        return intermediate

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
