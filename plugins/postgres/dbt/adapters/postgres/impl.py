from dataclasses import dataclass
from typing import Optional, Set, List, Any
from dbt.adapters.base.meta import available
from dbt.adapters.base.impl import AdapterConfig, ConstraintSupport
from dbt.adapters.postgres.index import PostgresIndexConfig
from dbt.adapters.sql import SQLAdapter
from dbt.adapters.postgres import PostgresConnectionManager
from dbt.adapters.postgres.column import PostgresColumn
from dbt.adapters.postgres import PostgresRelation
from dbt.contracts.graph.nodes import ConstraintType
from dbt.exceptions import (
    CrossDbReferenceProhibitedError,
    DbtRuntimeError,
    UnexpectedDbReferenceError,
)

# note that this isn't an adapter macro, so just a single underscore
GET_RELATIONS_MACRO_NAME = "postgres_get_relations"


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
