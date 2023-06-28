from collections import OrderedDict
from dataclasses import dataclass, field
from typing import Set, FrozenSet, List, Dict, Optional

import agate
from dbt.adapters.relation_configs import (
    MaterializationConfig,
    RelationConfigChangeset,
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.relation import ComponentName, RelationType
from dbt.exceptions import DbtRuntimeError
import dbt.utils

from dbt.adapters.postgres.relation_configs.index import (
    PostgresIndexConfig,
    PostgresIndexConfigChange,
)
from dbt.adapters.postgres.relation_configs.policy import (
    postgres_render,
    postgres_conform_part,
    MAX_CHARACTERS_IN_IDENTIFIER,
)
from dbt.adapters.postgres.relation_configs.schema import PostgresSchemaConfig


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class PostgresMaterializedViewConfig(MaterializationConfig, RelationConfigValidationMixin):
    """
    This config follows the specs found here:
    https://www.postgresql.org/docs/current/sql-creatematerializedview.html

    The following parameters are configurable by dbt:
    - name: name of the materialized view
    - schema: schema that contains the materialized view
    - query: the query that defines the view
    - indexes: the collection (set) of indexes on the materialized view

    Applicable defaults for non-configurable parameters:
    - method: `heap`
    - tablespace_name: `default_tablespace`
    - with_data: `True`
    """

    name: str
    schema: PostgresSchemaConfig
    query: str = field(hash=False, compare=False)
    indexes: FrozenSet[PostgresIndexConfig] = field(default_factory=frozenset)
    relation_type: Optional[RelationType] = RelationType.MaterializedView

    @property
    def fully_qualified_path(self) -> str:
        return postgres_render(
            OrderedDict(
                {
                    ComponentName.Database: self.database_name,
                    ComponentName.Schema: self.schema_name,
                    ComponentName.Identifier: self.name,
                }
            )
        )

    @property
    def fully_qualified_path_backup(self) -> str:
        return postgres_render(
            OrderedDict(
                {
                    ComponentName.Database: self.database_name,
                    ComponentName.Schema: self.schema_name,
                    ComponentName.Identifier: self.backup_name,
                }
            )
        )

    @property
    def fully_qualified_path_intermediate(self) -> str:
        return postgres_render(
            OrderedDict(
                {
                    ComponentName.Database: self.database_name,
                    ComponentName.Schema: self.schema_name,
                    ComponentName.Identifier: self.intermediate_name,
                }
            )
        )

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        """
        Validation rules at the materialized view level. All attribute level rules get run as a result of
        `RelationConfigValidationMixin`.

        Returns: a set of rules that should evaluate to `True` (i.e. False == validation failure)
        """
        return {
            RelationConfigValidationRule(
                validation_check=self.name is None
                or len(self.name) <= MAX_CHARACTERS_IN_IDENTIFIER,
                validation_error=DbtRuntimeError(
                    f"The materialized view name is more than {MAX_CHARACTERS_IN_IDENTIFIER} "
                    f"characters: {self.name}"
                ),
            ),
            RelationConfigValidationRule(
                validation_check=all({self.database_name, self.schema_name, self.name}),
                validation_error=DbtRuntimeError(
                    f"dbt-snowflake requires all three parts of an object's path, received:/n"
                    f"    database: {self.database_name}/n"
                    f"    schema: {self.schema_name}/n"
                    f"    identifier: {self.name}/n"
                ),
            ),
        }

    @classmethod
    def from_dict(cls, config_dict: dict) -> "PostgresMaterializedViewConfig":
        """
        Creates an instance of this class given the dict representation

        This is generally used indirectly by calling either `from_model_node()` or `from_relation_results()`

        Args:
            config_dict: a dict that aligns with the structure of this class, and it's attribute classes (e.g. indexes)

        Returns: an instance of this class
        """
        kwargs_dict = {
            "name": postgres_conform_part(ComponentName.Identifier, config_dict["name"]),
            "schema": PostgresSchemaConfig.from_dict(config_dict["schema"]),
            "query": config_dict["query"],
            "indexes": frozenset(
                PostgresIndexConfig.from_dict(index) for index in config_dict.get("indexes", {})
            ),
        }
        materialized_view: "PostgresMaterializedViewConfig" = super().from_dict(kwargs_dict)  # type: ignore
        return materialized_view

    @classmethod
    def parse_model_node(cls, model_node: ModelNode) -> dict:
        """
        Parse `RuntimeConfigObject.model` into a dict representation of a `PostgresMaterializedViewConfig` instance

        This is generally used indirectly by calling `from_model_node()`, but there are times when the dict
        version is more appropriate.

        Args:
            model_node: the `model` attribute from `config` in the jinja context

        Example `model_node`:

        ModelNode({
            ...,
            "compiled_code": "create materialized view my_materialized_view as select * from my_ref_table;",
            ...,
            "config" {
                ...,
                "extra": {
                    ...,
                    "indexes": [
                        {"columns": ["id"], "type": "hash", "unique": True},
                    ],
                    ...,
                },
                ...,
            },
            ...,
            "identifier": "my_materialized_view",
            ...,
        })

        Returns: a dict representation of an instance of this class that can be passed into `from_dict()`
        """
        indexes: List[dict] = model_node.config.extra.get("indexes", [])
        config_dict = {
            "name": model_node.identifier,
            "schema": PostgresSchemaConfig.parse_model_node(model_node),
            "query": (model_node.compiled_code or "").strip(),
            "indexes": [PostgresIndexConfig.parse_model_node(index) for index in indexes],
        }
        return config_dict

    @classmethod
    def parse_describe_relation_results(
        cls, describe_relation_results: Dict[str, agate.Table]
    ) -> dict:
        """
        Parse `RelationResults` into a dict representation of a `PostgresMaterializedViewConfig` instance

        This is generally used indirectly by calling `from_relation_results()`, but there are times when the dict
        version is more appropriate.

        Args:
            describe_relation_results: the results of a set of queries that fully describe an instance of this class

        Example of `relation_results`:

        {
            "materialized_view": agate.Table(agate.Row({
                "table_name": "my_materialized_view",
                "query": "create materialized view my_materialized_view as select * from my_ref_table;",
            })),
            "indexes": agate.Table([
                agate.Row({"columns": ["id"], "type": "hash", "unique": True}),
                ...,
            ],
        }

        Returns: a dict representation of an instance of this class that can be passed into `from_dict()`
        """
        materialized_view_config: agate.Table = describe_relation_results.get("materialized_view")
        materialized_view = materialized_view_config.rows[0]
        indexes: agate.Table = describe_relation_results["indexes"]

        config_dict = {
            "name": materialized_view["matviewname"],
            "schema": PostgresSchemaConfig.parse_describe_relation_results(materialized_view),
            "query": materialized_view["definition"].strip(),
            "indexes": [
                PostgresIndexConfig.parse_describe_relation_results(index)
                for index in indexes.rows
            ],
        }
        return config_dict

    def generate_index_name(self, index_fully_qualified_path) -> str:
        return dbt.utils.md5(
            "_".join(
                {
                    self.database_name,
                    self.schema_name,
                    self.name,
                    index_fully_qualified_path,
                }
            )
        )


@dataclass
class PostgresMaterializedViewConfigChangeset(RelationConfigChangeset):
    indexes: Set[PostgresIndexConfigChange] = field(default_factory=set)

    @property
    def requires_full_refresh(self) -> bool:
        return (
            any(index.requires_full_refresh for index in self.indexes)
            or super().requires_full_refresh
        )

    @property
    def is_empty(self) -> bool:
        return self.indexes == set() and super().is_empty
