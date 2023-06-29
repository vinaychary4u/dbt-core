from dbt.adapters.postgres.materialization_config.index import (
    PostgresIndexConfig,
    PostgresIndexConfigChange,
)
from dbt.adapters.postgres.materialization_config.materialized_view import (
    PostgresMaterializedViewConfig,
    PostgresMaterializedViewConfigChangeset,
)
from dbt.adapters.postgres.materialization_config.policy import (
    MAX_CHARACTERS_IN_IDENTIFIER,
    PostgresIncludePolicy,
    PostgresQuotePolicy,
    postgres_conform_part,
    postgres_render_part,
    postgres_render,
)
