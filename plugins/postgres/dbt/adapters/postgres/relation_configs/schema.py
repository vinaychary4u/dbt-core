from dataclasses import dataclass
from typing import Set

import agate
from dbt.adapters.relation_configs import (
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation_configs.base import PostgresRelationConfigBase
from dbt.adapters.postgres.relation_configs.database import PostgresDatabaseConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresSchemaConfig(PostgresRelationConfigBase, RelationConfigValidationMixin):
    """
    This config follow the specs found here:
    https://www.postgresql.org/docs/15/sql-createschema.html

    The following parameters are configurable by dbt:
    - name: name of the schema
    - database_name: name of the database
    """

    schema_name: str
    database: PostgresDatabaseConfig

    @property
    def database_name(self) -> str:
        return self.database.name

    @property
    def fully_qualified_path(self) -> str:
        return ".".join(
            part
            for part in [self.database.fully_qualified_path, self.schema_name]
            if part is not None
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        return {
            RelationConfigValidationRule(
                validation_check=len(self.schema_name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-postgres requires a name for a schema, received: {self.schema_name}"
                ),
            )
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PostgresSchemaConfig":
        kwargs_dict = {
            "schema_name": cls._render_part(ComponentName.Schema, config_dict["schema_name"]),
            "database": PostgresDatabaseConfig.from_dict(config_dict["database"]),
        }

        schema: "PostgresSchemaConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return schema

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        config_dict = {
            "schema_name": model_node.schema,
            "database": PostgresDatabaseConfig.parse_model_node(model_node),
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(cls, describe_relation_results: agate.Row) -> dict:
        config_dict = {
            "schema_name": describe_relation_results["schema_name"],
            "database": PostgresDatabaseConfig.parse_describe_relation_results(
                describe_relation_results
            ),
        }
        return config_dict
