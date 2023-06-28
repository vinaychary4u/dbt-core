from dbt.adapters.relation_configs._base import RelationConfig, DescribeRelationResults
from dbt.adapters.relation_configs._change import (
    RelationConfigChangeAction,
    RelationConfigChange,
    RelationConfigChangeset,
)
from dbt.adapters.relation_configs._database import DatabaseConfig
from dbt.adapters.relation_configs._materialization import MaterializationConfig
from dbt.adapters.relation_configs._policy import (
    IncludePolicy,
    QuotePolicy,
    conform_part,
    render_part,
    render,
)
from dbt.adapters.relation_configs._schema import SchemaConfig
from dbt.adapters.relation_configs._validation import (
    RelationConfigValidationMixin,
    RelationConfigValidationRule,
)
