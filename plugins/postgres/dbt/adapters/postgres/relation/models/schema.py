from dataclasses import dataclass
from typing import Set

from dbt.adapters.relation.models import SchemaRelation
from dbt.adapters.validation import ValidationMixin, ValidationRule
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation.models.database import PostgresDatabaseRelation
from dbt.adapters.postgres.relation.models.policy import PostgresRenderPolicy


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresSchemaRelation(SchemaRelation, ValidationMixin):
    """
    This config follow the specs found here:
    https://www.postgresql.org/docs/15/sql-createschema.html

    The following parameters are configurable by dbt:
    - name: name of the schema
    - database_name: name of the database
    """

    # attribution
    name: str

    # configuration
    render = PostgresRenderPolicy
    DatabaseParser = PostgresDatabaseRelation

    @classmethod
    def from_dict(cls, config_dict) -> "PostgresSchemaRelation":
        schema = super().from_dict(config_dict)
        assert isinstance(schema, PostgresSchemaRelation)
        return schema

    @property
    def validation_rules(self) -> Set[ValidationRule]:
        """
        Returns: a set of rules that should evaluate to `True` (i.e. False == validation failure)
        """
        return {
            ValidationRule(
                validation_check=len(self.name or "") > 0,
                validation_error=DbtRuntimeError(
                    f"dbt-postgres requires a name to reference a schema, received:\n"
                    f"    schema: {self.name}\n"
                ),
            ),
        }
