from dataclasses import replace
from typing import Dict, Set, Type

from dbt.contracts.graph.nodes import CompiledNode
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
    BACKUP_SUFFIX: str = "__dbt_backup"
    INTERMEDIATE_SUFFIX: str = "__dbt_tmp"

    def __init__(
        self,
        relation_models: Dict[RelationType, Type[models.Relation]],
        relation_changesets: Dict[RelationType, Type[models.RelationChangeset]],
        relation_can_be_renamed: Set[RelationType],
        render_policy: models.RenderPolicy,
    ):
        self.relation_models = relation_models
        self.relation_changesets = relation_changesets
        self.relation_can_be_renamed = relation_can_be_renamed
        self.render_policy = render_policy

    def make_from_node(self, node: CompiledNode) -> models.Relation:
        relation_type = RelationType(node.config.materialized)
        parser = self._get_relation_class(relation_type)
        relation = parser.from_node(node)
        assert isinstance(relation, models.Relation)  # mypy
        return relation

    def make_from_describe_relation_results(
        self,
        describe_relation_results: models.DescribeRelationResults,
        relation_type: RelationType,
    ) -> models.Relation:
        parser = self._get_relation_class(relation_type)
        relation = parser.from_describe_relation_results(describe_relation_results)
        assert isinstance(relation, models.Relation)  # mypy
        return relation

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

    def make_backup_ref(self, existing_relation: models.Relation) -> models.RelationRef:
        if existing_relation.can_be_renamed:
            backup_name = self.render_policy.part(
                ComponentName.Identifier, f"{existing_relation.name}{self.BACKUP_SUFFIX}"
            )
            assert isinstance(backup_name, str)  # mypy
            return self.make_ref(
                name=backup_name,
                schema_name=existing_relation.schema_name,
                database_name=existing_relation.database_name,
                relation_type=existing_relation.type,
            )
        raise DbtRuntimeError(
            f"This relation cannot be renamed, hence it cannot be backed up: \n"
            f"    path: {existing_relation.fully_qualified_path}\n"
            f"    type: {existing_relation.type}\n"
        )

    def make_intermediate(self, target_relation: models.Relation) -> models.Relation:
        if target_relation.can_be_renamed:
            intermediate_name = self.render_policy.part(
                ComponentName.Identifier, f"{target_relation.name}{self.INTERMEDIATE_SUFFIX}"
            )
            return replace(target_relation, name=intermediate_name)
        raise DbtRuntimeError(
            f"This relation cannot be renamed, hence it cannot be staged: \n"
            f"    path: {target_relation.fully_qualified_path}\n"
            f"    type: {target_relation.type}\n"
        )

    def make_changeset(
        self, existing_relation: models.Relation, target_relation: models.Relation
    ) -> models.RelationChangeset:
        changeset = self._get_changeset(existing_relation.type)
        return changeset.from_relations(existing_relation, target_relation)

    def _get_relation_class(self, relation_type: RelationType) -> Type[models.Relation]:
        if relation := self.relation_models.get(relation_type):
            return relation
        raise DbtRuntimeError(
            f"This factory does not have a relation for this type.\n"
            f"    received: {relation_type}\n"
            f"    options: {', '.join(t for t in self.relation_models.keys())}\n"
        )

    def _get_changeset(self, relation_type: RelationType) -> Type[models.RelationChangeset]:
        if relation_changeset := self.relation_changesets.get(relation_type):
            return relation_changeset
        raise DbtRuntimeError(
            f"This factory does not have a relation changeset for this type.\n"
            f"    received: {relation_type}\n"
            f"    options: {', '.join(t for t in self.relation_changesets.keys())}\n"
        )
