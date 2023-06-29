from collections import OrderedDict
from dataclasses import dataclass
from typing import Set

import agate
from dbt.adapters.materialization_config import (
    SchemaConfig,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.materialization_config.database import PostgresDatabaseConfig
from dbt.adapters.postgres.materialization_config.policy import (
    postgres_render,
    postgres_conform_part,
)


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresSchemaConfig(SchemaConfig, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://www.postgresql.org/docs/15/sql-createschema.html

    The following parameters are configurable by dbt:
    - name: name of the schema
    - database_name: name of the database
    """

    name: str
    database: PostgresDatabaseConfig

    @property
    def fully_qualified_path(self) -> str:
        return postgres_render(
            OrderedDict(
                {ComponentName.Database: self.database_name, ComponentName.Schema: self.name}
            )
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-postgres requires a name for a schema, received: {self.name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "SchemaConfig":
        kwargs_dict = {
            "name": postgres_conform_part(ComponentName.Schema, config_dict["name"]),
            "database": PostgresDatabaseConfig.from_dict(config_dict["database"]),
        }
        schema: "SchemaConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return schema

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "name": model_node.schema,
            "database": PostgresDatabaseConfig.parse_model_node(model_node),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        config_dict = {
            "name": describe_relation_results["schemaname"],
            "database": PostgresDatabaseConfig.parse_describe_relation_results(
                describe_relation_results
            ),
        }
        return config_dict
