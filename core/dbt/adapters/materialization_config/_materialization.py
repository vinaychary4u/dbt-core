from abc import ABC
from dataclasses import dataclass

from dbt.adapters.materialization_config._base import DescribeRelationResults, RelationConfig
from dbt.adapters.materialization_config._database import DatabaseConfig
from dbt.adapters.materialization_config._schema import SchemaConfig
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import RelationType


@dataclass(frozen=True)
class MaterializationConfig(RelationConfig, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str
    schema: "SchemaConfig"
    query: str
    type: RelationType

    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def database(self) -> "DatabaseConfig":
        return self.schema.database

    @property
    def database_name(self) -> str:
        return self.database.name

    @property
    def fully_qualified_path(self) -> str:
        """
        This is sufficient if there is no quote policy or include policy, otherwise override it to apply those policies.

        Returns: a fully qualified path, run through the quote and include policies, for rendering in a template
        """
        return f"{self.schema.fully_qualified_path}.{self.name}"

    @classmethod
    def from_dict(cls, kwargs_dict) -> "MaterializationConfig":
        """
        Supports type annotations
        """
        config = super().from_dict(kwargs_dict)
        assert isinstance(config, MaterializationConfig)
        return config

    @classmethod
    def from_model_node(cls, model_node: ModelNode) -> "MaterializationConfig":
        """
        Supports type annotations
        """
        config = super().from_model_node(model_node)
        assert isinstance(config, MaterializationConfig)
        return config

    @classmethod
    def from_describe_relation_results(
        cls, describe_relation_results: DescribeRelationResults
    ) -> "MaterializationConfig":
        """
        Supports type annotations
        """
        config = super().from_describe_relation_results(describe_relation_results)
        assert isinstance(config, MaterializationConfig)
        return config

    def __str__(self):
        """
        Useful for template rendering and aligns with BaseRelation so that they are interchangeable
        """
        return self.fully_qualified_path
