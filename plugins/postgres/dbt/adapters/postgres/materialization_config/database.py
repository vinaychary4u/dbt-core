from collections import OrderedDict
from dataclasses import dataclass
from typing import Set

import agate
from dbt.adapters.materialization_config import (
    DatabaseConfig,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.materialization_config.policy import (
    postgres_render,
    postgres_conform_part,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresDatabaseConfig(DatabaseConfig, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://www.postgresql.org/docs/current/sql-createdatabase.html

    The following parameters are configurable by dbt:
    - name: name of the database
    """

    name: str

    @property
    def fully_qualified_path(self) -> str:
        return postgres_render(OrderedDict({ComponentName.Database: self.name}))

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-postgres requires a name for a database, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "DatabaseConfig":
        """
        Because this returns a frozen dataclass, this method should be overridden if additional parameters are supplied.
        """
        kwargs_dict = {"name": postgres_conform_part(ComponentName.Database, config_dict["name"])}
        database: "DatabaseConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return database

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Because this returns a `dict`, this method should be extended if additional parameters are supplied.
        """
        config_dict = {"name": model_node.database}
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        """
        Because this returns a `dict`, this method should be extended if additional parameters are supplied.
        """
        config_dict = {"name": describe_relation_results["databasename"]}
        return config_dict
