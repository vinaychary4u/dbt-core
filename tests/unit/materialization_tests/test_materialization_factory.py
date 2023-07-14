from dbt.adapters.materialization.models import MaterializationType
from dbt.contracts.relation import RelationType


def test_make_from_node(materialization_factory, materialized_view_compiled_node):
    materialization = materialization_factory.make_from_node(
        node=materialized_view_compiled_node,
        existing_relation_ref=None,
    )
    assert materialization.type == MaterializationType.MaterializedView

    materialized_view = materialization.target_relation
    assert materialized_view.type == RelationType.MaterializedView

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"
