"""
Uses the following fixtures in `unit/conftest.py`:
- `relation_factory`
- `materialized_view_ref`
"""
import pytest

from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError


def test_make_ref(materialized_view_ref):
    assert materialized_view_ref.name == "my_materialized_view"
    assert materialized_view_ref.schema_name == "my_schema"
    assert materialized_view_ref.database_name == "my_database"
    assert materialized_view_ref.type == "materialized_view"
    assert materialized_view_ref.can_be_renamed is False


def test_make_backup_ref(relation_factory, materialized_view_ref):
    if materialized_view_ref.can_be_renamed:
        assert materialized_view_ref.name == '"my_materialized_view__dbt_backup"'
    else:
        with pytest.raises(DbtRuntimeError):
            relation_factory.make_backup_ref(materialized_view_ref)


def test_make_intermediate(relation_factory, materialized_view_ref):
    if materialized_view_ref.can_be_renamed:
        intermediate_relation = relation_factory.make_intermediate(materialized_view_ref)
        assert intermediate_relation.name == '"my_materialized_view__dbt_tmp"'
    else:
        with pytest.raises(DbtRuntimeError):
            relation_factory.make_intermediate(materialized_view_ref)


def test_make_from_describe_relation_results(
    relation_factory, materialized_view_describe_relation_results
):
    materialized_view = relation_factory.make_from_describe_relation_results(
        materialized_view_describe_relation_results, RelationType.MaterializedView
    )

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"


def test_make_from_node(relation_factory, materialized_view_compiled_node):
    materialized_view = relation_factory.make_from_node(materialized_view_compiled_node)

    assert materialized_view.name == "my_materialized_view"
    assert materialized_view.schema_name == "my_schema"
    assert materialized_view.database_name == "my_database"
    assert materialized_view.query == "select 42 from meaning_of_life"
