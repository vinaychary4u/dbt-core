from dbt.adapters.materialization.models import MaterializationType
from dbt.contracts.relation import RelationType

from dbt.adapters.postgres.relation import models as relation_models


def test_make_from_runtime_config(materialization_factory, materialized_view_runtime_config):
    materialization = materialization_factory.make_from_runtime_config(
        runtime_config=materialized_view_runtime_config,
        materialization_type=MaterializationType.MaterializedView,
        existing_relation_ref=None,
    )
    assert materialization.type == MaterializationType.MaterializedView

    materialized_view = materialization.target_relation
    assert materialized_view.type == RelationType.MaterializedView

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"

    index_1 = relation_models.PostgresIndexRelation(
        column_names=frozenset({"id", "value"}),
        method=relation_models.PostgresIndexMethod.hash,
        unique=False,
        render=relation_models.PostgresRenderPolicy,
    )
    index_2 = relation_models.PostgresIndexRelation(
        column_names=frozenset({"id"}),
        method=relation_models.PostgresIndexMethod.btree,
        unique=True,
        render=relation_models.PostgresRenderPolicy,
    )
    assert index_1 in materialized_view.indexes
    assert index_2 in materialized_view.indexes
