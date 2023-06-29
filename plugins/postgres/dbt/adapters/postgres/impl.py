from datetime import datetime
from dataclasses import dataclass
from typing import Any, FrozenSet, List, Optional, Set

from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.relation_configs import MaterializationConfig, RelationConfigChangeAction
from dbt.adapters.sql import SQLAdapter
from dbt.contracts.graph.nodes import ConstraintType
from dbt.contracts.relation import ComponentName, RelationType
from dbt.dataclass_schema import dbtClassMixin, ValidationError
from dbt.exceptions import (
    CrossDbReferenceProhibitedError,
    IndexConfigNotDictError,
    IndexConfigError,
    DbtRuntimeError,
    UnexpectedDbReferenceError,
)
import dbt.utils

from dbt.adapters.postgres import PostgresConnectionManager, PostgresRelation
from dbt.adapters.postgres.column import PostgresColumn
from dbt.adapters.postgres.relation_configs import (
    PostgresIndexConfig as PostgresIndexConfigMatView,
    PostgresIndexConfigChange,
    PostgresMaterializedViewConfig,
    PostgresMaterializedViewConfigChangeset,
    PostgresIncludePolicy,
    PostgresQuotePolicy,
    postgres_conform_part,
)


# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = "postgres_get_relations"


@dataclass
class PostgresIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = None

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.utcnow().isoformat()
        inputs = self.columns + [relation.render(), str(self.unique), str(self.type), now]
        string = "_".join(inputs)
        return dbt.utils.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["PostgresIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)


@dataclass
class PostgresConfig(AdapterConfig):
    unlogged: Optional[bool] = None
    indexes: Optional[List[PostgresIndexConfig]] = None


class PostgresAdapter(SQLAdapter):
    Relation = PostgresRelation
    ConnectionManager = PostgresConnectionManager
    Column = PostgresColumn

    AdapterSpecificConfigs = PostgresConfig

    CONSTRAINT_SUPPORT = {
        ConstraintType.check: ConstraintSupport.ENFORCED,
        ConstraintType.not_null: ConstraintSupport.ENFORCED,
        ConstraintType.unique: ConstraintSupport.ENFORCED,
        ConstraintType.primary_key: ConstraintSupport.ENFORCED,
        ConstraintType.foreign_key: ConstraintSupport.ENFORCED,
    }
    materialization_configs = {RelationType.MaterializedView: PostgresMaterializedViewConfig}
    include_policy = PostgresIncludePolicy()
    quote_policy = PostgresQuotePolicy()

    @classmethod
    def date_function(cls):
        return "now()"

    @available
    def verify_database(self, database):
        if database.startswith('"'):
            database = database.strip('"')
        expected = self.config.credentials.database
        if database.lower() != expected.lower():
            raise UnexpectedDbReferenceError(self.type(), database, expected)
        # return an empty string on success so macros can call this
        return ""

    @available
    def parse_index(self, raw_index: Any) -> Optional[PostgresIndexConfig]:
        return PostgresIndexConfig.parse(raw_index)

    def _link_cached_database_relations(self, schemas: Set[str]):
        """
        :param schemas: The set of schemas that should have links added.
        """
        database = self.config.credentials.database
        table = self.execute_macro(GET_RELATIONS_MACRO_NAME)

        for (dep_schema, dep_name, refed_schema, refed_name) in table:
            dependent = self.Relation.create(
                database=database, schema=dep_schema, identifier=dep_name
            )
            referenced = self.Relation.create(
                database=database, schema=refed_schema, identifier=refed_name
            )

            # don't record in cache if this relation isn't in a relevant
            # schema
            if refed_schema.lower() in schemas:
                self.cache.add_link(referenced, dependent)

    def _get_catalog_schemas(self, manifest):
        # postgres only allow one database (the main one)
        schemas = super()._get_catalog_schemas(manifest)
        try:
            return schemas.flatten()
        except DbtRuntimeError as exc:
            raise CrossDbReferenceProhibitedError(self.type(), exc.msg)

    def _link_cached_relations(self, manifest):
        schemas: Set[str] = set()
        relations_schemas = self._get_cache_schemas(manifest)
        for relation in relations_schemas:
            self.verify_database(relation.database)
            schemas.add(relation.schema.lower())

        self._link_cached_database_relations(schemas)

    def _relations_cache_for_schemas(self, manifest, cache_schemas=None):
        super()._relations_cache_for_schemas(manifest, cache_schemas)
        self._link_cached_relations(manifest)

    def timestamp_add_sql(self, add_to: str, number: int = 1, interval: str = "hour") -> str:
        return f"{add_to} + interval '{number} {interval}'"

    def valid_incremental_strategies(self):
        """The set of standard builtin strategies which this adapter supports out-of-the-box.
        Not used to validate custom strategies defined by end users.
        """
        return ["append", "delete+insert"]

    def debug_query(self):
        self.execute("select 1 as id")

    @available
    @classmethod
    def materialized_view_config_changeset(
        cls,
        existing_materialized_view: PostgresMaterializedViewConfig,
        new_materialized_view: PostgresMaterializedViewConfig,
    ) -> PostgresMaterializedViewConfigChangeset:
        try:
            assert isinstance(existing_materialized_view, PostgresMaterializedViewConfig)
            assert isinstance(new_materialized_view, PostgresMaterializedViewConfig)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two materialized view configs were expected, but received:"
                f"/n    {existing_materialized_view}"
                f"/n    {new_materialized_view}"
            )

        config_changeset = PostgresMaterializedViewConfigChangeset()

        config_changeset.indexes = cls.index_config_changeset(
            existing_materialized_view.indexes, new_materialized_view.indexes
        )

        if config_changeset.is_empty and existing_materialized_view != new_materialized_view:
            # we need to force a full refresh if we didn't detect any changes but the objects are not the same
            config_changeset.force_full_refresh()

        return config_changeset

    @available
    @classmethod
    def index_config_changeset(
        cls,
        existing_indexes: FrozenSet[PostgresIndexConfigMatView],
        new_indexes: FrozenSet[PostgresIndexConfigMatView],
    ) -> Set[PostgresIndexConfigChange]:
        """
        Get the index updates that will occur as a result of a new run

        There are four scenarios:

        1. Indexes are equal -> don't return these
        2. Index is new -> create these
        3. Index is old -> drop these
        4. Indexes are not equal -> drop old, create new -> two actions

        Returns: a set of index updates in the form {"action": "drop/create", "context": <IndexConfig>}
        """
        drop_changes = set(
            PostgresIndexConfigChange(action=RelationConfigChangeAction.drop, context=index)
            for index in existing_indexes.difference(new_indexes)
        )
        create_changes = set(
            PostgresIndexConfigChange(action=RelationConfigChangeAction.create, context=index)
            for index in new_indexes.difference(existing_indexes)
        )
        return set().union(drop_changes, create_changes)

    @available
    @classmethod
    def generate_index_name(
        cls,
        materialization_config: MaterializationConfig,
        index_config: PostgresIndexConfigMatView,
    ) -> str:
        return dbt.utils.md5(
            "_".join(
                {
                    postgres_conform_part(
                        ComponentName.Database, materialization_config.database_name
                    ),
                    postgres_conform_part(
                        ComponentName.Schema, materialization_config.schema_name
                    ),
                    postgres_conform_part(ComponentName.Identifier, materialization_config.name),
                    *sorted(
                        postgres_conform_part(ComponentName.Identifier, column)
                        for column in index_config.column_names
                    ),
                    str(index_config.unique),
                    str(index_config.method),
                    str(datetime.utcnow().isoformat()),
                }
            )
        )
