from abc import ABC
from dataclasses import dataclass

from dbt.contracts.relation import RelationType
from dbt.adapters.relation_configs._base import RelationConfig
from dbt.adapters.relation_configs._database import DatabaseConfig
from dbt.adapters.relation_configs._schema import SchemaConfig


@dataclass(frozen=True)
class MaterializationConfig(RelationConfig, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str
    schema: "SchemaConfig"
    query: str
    relation_type: RelationType

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
    def backup_name(self) -> str:
        """
        Used for hot-swapping during replacement

        Returns:
            a name unique to this materialized view
        """
        return f"{self.name}__dbt_backup"

    @property
    def intermediate_name(self) -> str:
        """
        Used for hot-swapping during replacement

        Returns:
            a name unique to this materialized view
        """
        return f"{self.name}__dbt_tmp"

    @property
    def fully_qualified_path(self) -> str:
        """
        This is sufficient if there is no quote policy or include policy, otherwise override it to apply those policies.

        Returns: a fully qualified path, run through the quote and include policies, for rendering in a template
        """
        return f"{self.schema.fully_qualified_path}.{self.name}"

    @property
    def fully_qualified_path_backup(self) -> str:
        """
        This is sufficient if there is no quote policy or include policy, otherwise override it to apply those policies.

        Returns: a fully qualified path, run through the quote and include policies, for rendering in a template
        """
        return f"{self.schema.fully_qualified_path}.{self.backup_name}"

    @property
    def fully_qualified_path_intermediate(self) -> str:
        """
        This is sufficient if there is no quote policy or include policy, otherwise override it to apply those policies.

        Returns: a fully qualified path, run through the quote and include policies, for rendering in a template
        """
        return f"{self.schema.fully_qualified_path}.{self.intermediate_name}"
