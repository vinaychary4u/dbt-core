from dbt.adapters.postgres.relation_configs.index import (
    PostgresIndexConfig,
    PostgresIndexConfigChange,
)
from dbt.adapters.postgres.relation_configs.materialized_view import (
    PostgresMaterializedViewConfig,
    PostgresMaterializedViewConfigChangeset,
)
from dbt.adapters.postgres.relation_configs.policy import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    PostgresIncludePolicy,
    PostgresQuotePolicy,
    postgres_conform_part,
    postgres_render_part,
    postgres_render,
)
