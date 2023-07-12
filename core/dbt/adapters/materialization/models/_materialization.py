from abc import ABC
from dataclasses import dataclass, field
from typing import Optional

from dbt.adapters.relation.factory import RelationFactory
from dbt.adapters.relation.models import DescribeRelationResults, Relation, RelationRef
from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.dataclass_schema import StrEnum
from dbt.flags import get_flag_obj
from dbt.utils import filter_null_values


class MaterializationType(StrEnum):
    """
    This overlaps with `RelationType` for several values (e.g. `View`); however, they are not the same.
    For example, a materialization type of `Incremental` would be associated with a relation type of `Table`.
    """

    View = "view"
    Table = "table"
    Incremental = "incremental"
    Seed = "seed"
    MaterializedView = "materialized_view"


class MaterializationBuildStrategy(StrEnum):
    Alter = "alter"
    Create = "create"
    NoOp = "no_op"
    Replace = "replace"


@dataclass
class Materialization(ABC):

    type: MaterializationType
    relation_factory: RelationFactory
    target_relation: Relation
    existing_relation_ref: Optional[RelationRef] = None
    is_full_refresh: bool = False
    grants: dict = field(default_factory=dict)
    on_configuration_change: OnConfigurationChangeOption = OnConfigurationChangeOption.default()

    def __str__(self) -> str:
        """
        This gets used in some error messages.

        Returns:
            A user-friendly name to be used in logging, error messages, etc.
        """
        return str(self.target_relation)

    def existing_relation(
        self, describe_relation_results: DescribeRelationResults
    ) -> Optional[Relation]:
        """
        Produce a full-blown `Relation` instance for `self.existing_relation_ref` using metadata from the database

        Args:
            describe_relation_results: the results from the macro `describe_sql(self.existing_relation_ref)`

        Returns:
            a `Relation` instance that represents `self.existing_relation_ref` in the database
        """
        if self.existing_relation_ref:
            relation_type = self.existing_relation_ref.type
            return self.relation_factory.make_from_describe_relation_results(
                describe_relation_results, relation_type
            )
        return None

    @property
    def intermediate_relation(self) -> Optional[Relation]:
        if self.target_relation:
            return self.relation_factory.make_intermediate(self.target_relation)
        return None

    @property
    def backup_relation_ref(self) -> Optional[RelationRef]:
        if self.existing_relation_ref:
            return self.relation_factory.make_backup_ref(self.existing_relation_ref)
        return None

    @property
    def build_strategy(self) -> MaterializationBuildStrategy:
        return MaterializationBuildStrategy.NoOp

    @property
    def should_revoke_grants(self) -> bool:
        """
        This attempts to mimic the macro `should_revoke()`
        """
        should_revoke = {
            MaterializationBuildStrategy.Alter: True,
            MaterializationBuildStrategy.Create: False,
            MaterializationBuildStrategy.NoOp: False,
            MaterializationBuildStrategy.Replace: True,
        }
        return should_revoke[self.build_strategy]

    @classmethod
    def from_dict(cls, config_dict) -> "Materialization":
        return cls(**filter_null_values(config_dict))

    @classmethod
    def from_runtime_config(
        cls,
        runtime_config,
        relation_factory: RelationFactory,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> "Materialization":
        config_dict = cls.parse_runtime_config(
            runtime_config, relation_factory, existing_relation_ref
        )
        materialization = cls.from_dict(config_dict)
        return materialization

    @classmethod
    def parse_runtime_config(
        cls,
        runtime_config,
        relation_factory: RelationFactory,
        existing_relation_ref: Optional[RelationRef] = None,
    ) -> dict:
        target_relation = relation_factory.make_from_model_node(runtime_config.model)
        # FULL_REFRESH defaults to False, hence the default in runtime_config.get()
        is_full_refresh = any(
            {get_flag_obj().FULL_REFRESH, runtime_config.get("full_refresh", False)}
        )
        grants = runtime_config.get("grants", {})
        on_configuration_change = runtime_config.get(
            "on_configuration_change", OnConfigurationChangeOption.default()
        )

        return {
            "relation_factory": relation_factory,
            "target_relation": target_relation,
            "is_full_refresh": is_full_refresh,
            "grants": grants,
            "on_configuration_change": on_configuration_change,
            "existing_relation_ref": existing_relation_ref,
        }
