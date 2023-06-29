import dataclasses
from typing import Dict, Type

from dbt.adapters.materialization_config import (
    DescribeRelationResults,
    MaterializationConfig,
    RelationConfigChangeAction,
)
from dbt.contracts.graph.model_config import OnConfigurationChangeOption
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import Policy, RelationType as _RelationType
from dbt.exceptions import DbtRuntimeError


class Materialization:
    """
    This class is a service layer version of `BaseRelation` that exposes `MaterializationConfig`
    functionality on `BaseAdapter`.
    """

    # registers MaterializationConfigs to RelationTypes
    materialization_configs: Dict[_RelationType, MaterializationConfig] = dataclasses.field(
        default_factory=dict
    )

    include_policy: Policy = Policy()
    quote_policy: Policy = Policy()

    # useful Enums for templating
    ChangeAction: Type[RelationConfigChangeAction] = RelationConfigChangeAction
    ChangeOption: Type[OnConfigurationChangeOption] = OnConfigurationChangeOption
    RelationType: Type[_RelationType] = _RelationType

    @classmethod
    def make_backup(cls, materialization_config: MaterializationConfig) -> MaterializationConfig:
        """
        Return a copy of the materialization config, but with a backup name instead of the original name.

        Args:
            materialization_config: the materialization that needs a backup

        Returns:
            a renamed copy of the materialization config
        """
        return dataclasses.replace(
            materialization_config, name=cls.backup_name(materialization_config)
        )

    @classmethod
    def make_intermediate(
        cls, materialization_config: MaterializationConfig
    ) -> MaterializationConfig:
        """
        Return a copy of the materialization config, but with a backup name instead of the original name.

        Args:
            materialization_config: the materialization that needs a backup

        Returns:
            a renamed copy of the materialization config
        """
        return dataclasses.replace(
            materialization_config, name=cls.intermediate_name(materialization_config)
        )

    @classmethod
    def backup_name(cls, materialization_config: MaterializationConfig) -> str:
        """
        Mimic the macro `make_backup_relation()` for `MaterializationConfig` instances.
        """
        return f"{materialization_config.name}__dbt_backup"

    @classmethod
    def intermediate_name(cls, materialization_config: MaterializationConfig) -> str:
        """
        Mimic the macro `make_intermediate_relation()` for `MaterializationConfig` instances
        """
        return f"{materialization_config.name}__dbt_tmp"

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> MaterializationConfig:
        """
        Produce a validated materialization config from the config available in the global jinja context.

        The intention is to remove validation from the jinja context and put it in python. This method gets
        called in a jinja template and it's results are used in the jinja template. For an example, please
        refer to `dbt/include/global_project/macros/materializations/models/materialized_view/materialization.sql`.
        In this file, the relation config is retrieved right away, to ensure that the config is validated before
        any sql is executed against the database.

        Args:
            model_node: the `model` ModelNode instance that's in the global jinja context

        Returns: a validated adapter-specific, relation_type-specific MaterializationConfig instance
        """
        relation_type = _RelationType(model_node.config.materialized)

        if materialization_config := cls.materialization_configs.get(relation_type):
            return materialization_config.from_model_node(model_node)

        raise DbtRuntimeError(
            f"materialization_config_from_model_node() is not supported"
            f" for the provided relation type: {relation_type}"
        )

    @classmethod
    def from_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults, relation_type: _RelationType
    ) -> MaterializationConfig:
        """
        Produce a validated materialization config from a series of "describe <relation>"-type queries.

        The intention is to remove validation from the jinja context and put it in python. This method gets
        called in a jinja template and it's results are used in the jinja template. For an example, please
        refer to `dbt/include/global_project/macros/materializations/models/materialized_view/materialization.sql`.

        Args:
            describe_relation_results: the results of one or more queries run against the database
                to describe this relation
            relation_type: the type of relation associated with the relation results

        Returns: a validated adapter-specific, relation_type-specific MaterializationConfig instance
        """
        if materialization_config := cls.materialization_configs.get(relation_type):
            return materialization_config.from_describe_relation_results(describe_relation_results)

        raise DbtRuntimeError(
            f"materialization_config_from_describe_relation_results() is not"
            f" supported for the provided relation type: {relation_type}"
        )
