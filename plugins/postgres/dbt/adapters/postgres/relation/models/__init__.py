from dbt.adapters.postgres.relation.models._index import (
    PostgresIndexRelation,
    PostgresIndexRelationChange,
)
from dbt.adapters.postgres.relation.models._materialized_view import (
    PostgresMaterializedViewRelation,
    PostgresMaterializedViewRelationChangeset,
)
from dbt.adapters.postgres.relation.models._policy import (
    MAX_CHARACTERS_IN_IDENTIFIER,
)
