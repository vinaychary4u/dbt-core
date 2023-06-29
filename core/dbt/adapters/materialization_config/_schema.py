from abc import ABC
from dataclasses import dataclass

from dbt.adapters.materialization_config._base import RelationConfig
from dbt.adapters.materialization_config._database import DatabaseConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class SchemaConfig(RelationConfig, ABC):
    """
    This config identifies the minimal schema parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str
    database: DatabaseConfig

    @property
    def database_name(self) -> str:
        return self.database.name

    @property
    def fully_qualified_path(self) -> str:
        """
        This is sufficient if there is no quote policy or include policy, otherwise override it to apply those policies.

        Returns: a fully qualified path, run through the quote and include policies, for rendering in a template
        """
        return f"{self.database.fully_qualified_path}.{self.name}"
