from abc import ABC
from dataclasses import dataclass

from dbt.adapters.materialization_config._base import RelationConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class DatabaseConfig(RelationConfig, ABC):
    """
    This config identifies the minimal database parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    name: str

    @property
    def fully_qualified_path(self) -> str:
        """
        This is sufficient if there is no quote policy or include policy, otherwise override it to apply those policies.

        Returns: a fully qualified path, run through the quote and include policies, for rendering in a template
        """
        return self.name
