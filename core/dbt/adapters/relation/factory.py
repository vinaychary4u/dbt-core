from dataclasses import replace
from typing import Dict, FrozenSet, Type

from dbt.contracts.graph.nodes import ParsedNode
from dbt.contracts.relation import ComponentName, RelationType
from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.relation.models import (
    DescribeRelationResults,
    MaterializedViewRelation,
    MaterializedViewRelationChangeset,
    Relation,
    RelationChangeset,
    RelationRef,
    RenderPolicy,
)


class RelationFactory:
    """
    Unlike other classes that get used by adapters, this class is not intended to be subclassed. Instead,
    an instance should be taken from it that takes in all the required configuration (or defaults to
    what is here).
    """

    # this configuration should never change
    BACKUP_SUFFIX: str = "__dbt_backup"
    INTERMEDIATE_SUFFIX: str = "__dbt_tmp"

    def __init__(self, **kwargs):
        # the `StrEnum` class will generally be `RelationType`, however this allows for extending that Enum
        self.relation_types: Type[StrEnum] = kwargs.get("relation_types", RelationType)
        self.relation_models: Dict[StrEnum, Type[Relation]] = kwargs.get(
            "relation_models",
            {
                RelationType.MaterializedView: MaterializedViewRelation,
            },
        )
        self.relation_changesets: Dict[StrEnum, Type[RelationChangeset]] = kwargs.get(
            "relation_changesets",
            {
                RelationType.MaterializedView: MaterializedViewRelationChangeset,
            },
        )
        self.relation_can_be_renamed: FrozenSet[StrEnum] = kwargs.get(
            "relation_can_be_renamed", {frozenset()}
        )
        self.render_policy: RenderPolicy = kwargs.get("render_policy", RenderPolicy())

        try:
            for relation_type in self.relation_models.keys():
                assert relation_type in self.relation_types
        except AssertionError:
            raise DbtRuntimeError(
                f"Received models for {relation_type} "
                f"but these relation types are not registered on this factory.\n"
                f"    registered relation types: {', '.join(self.relation_types)}\n"
            )

        try:
            for relation_type in self.relation_changesets.keys():
                assert relation_type in self.relation_types
        except AssertionError:
            raise DbtRuntimeError(
                f"Received changeset for {relation_type}"
                f"but this relation type is not registered on this factory.\n"
                f"    registered relation types: {', '.join(self.relation_types)}\n"
            )

    def make_from_node(self, node: ParsedNode) -> Relation:
        relation_type = self.relation_types(node.config.materialized)
        parser = self._get_relation_model(relation_type)
        relation = parser.from_node(node)
        assert isinstance(relation, Relation)  # mypy
        return relation

    def make_from_describe_relation_results(
        self,
        describe_relation_results: DescribeRelationResults,
        relation_type: str,
    ) -> Relation:
        model = self._get_relation_model(self.relation_types(relation_type))
        relation = model.from_describe_relation_results(describe_relation_results)
        assert isinstance(relation, Relation)  # mypy
        return relation

    def make_ref(
        self,
        name: str,
        schema_name: str,
        database_name: str,
        relation_type: str,
    ) -> RelationRef:
        relation_type = self._get_relation_type(relation_type)
        relation_ref = RelationRef.from_dict(
            {
                "name": name,
                "schema": {
                    "name": schema_name,
                    "database": {
                        "name": database_name,
                    },
                },
                "render": self.render_policy,
                "type": relation_type,
                "can_be_renamed": relation_type in self.relation_can_be_renamed,
            }
        )
        return relation_ref

    def make_backup_ref(self, existing_relation: Relation) -> RelationRef:
        if existing_relation.can_be_renamed:
            backup_name = self.render_policy.part(
                ComponentName.Identifier, f"{existing_relation.name}{self.BACKUP_SUFFIX}"
            )
            assert isinstance(
                backup_name, str
            )  # since `part` can return None in certain scenarios (not this one)
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

    def make_intermediate(self, target_relation: Relation) -> Relation:
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
        self, existing_relation: Relation, target_relation: Relation
    ) -> RelationChangeset:
        changeset = self._get_relation_changeset(existing_relation.type)
        return changeset.from_relations(existing_relation, target_relation)

    def _get_relation_type(self, relation_type: str) -> StrEnum:
        try:
            return self.relation_types(relation_type)
        except ValueError:
            raise DbtRuntimeError(
                f"This factory does not recognize this relation type.\n"
                f"    received: {relation_type}\n"
                f"    options: {', '.join(t for t in self.relation_types)}\n"
            )

    def _get_relation_model(self, relation_type: StrEnum) -> Type[Relation]:
        if relation := self.relation_models.get(relation_type):
            return relation
        raise DbtRuntimeError(
            f"This factory does not have a relation model for this type.\n"
            f"    received: {relation_type}\n"
            f"    options: {', '.join(t for t in self.relation_models.keys())}\n"
        )

    def _get_relation_changeset(self, relation_type: StrEnum) -> Type[RelationChangeset]:
        if relation_changeset := self.relation_changesets.get(relation_type):
            return relation_changeset
        raise DbtRuntimeError(
            f"This factory does not have a relation changeset for this type.\n"
            f"    received: {relation_type}\n"
            f"    options: {', '.join(t for t in self.relation_changesets.keys())}\n"
        )
