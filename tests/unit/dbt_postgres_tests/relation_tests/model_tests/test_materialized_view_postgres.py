from dataclasses import replace
from typing import Type

import pytest

from dbt.adapters.relation.models import RelationChangeAction
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.postgres.relation.models import (
    PostgresIndexRelation,
    PostgresIndexRelationChange,
    PostgresMaterializedViewRelation,
    PostgresMaterializedViewRelationChangeset,
)


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        (
            {
                "name": "my_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 1 from my_favoriate_table",
            },
            None,
        ),
        (
            {
                "name": "my_indexed_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                "query": "select 42 from meaning_of_life",
                "indexes": [
                    {
                        "column_names": frozenset({"id", "value"}),
                        "method": "hash",
                        "unique": False,
                    },
                    {"column_names": frozenset({"id"}), "method": "btree", "unique": True},
                ],
            },
            None,
        ),
        (
            {
                "my_name": "my_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
            },
            DbtRuntimeError,
        ),
    ],
)
def test_create_materialized_view(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            PostgresMaterializedViewRelation.from_dict(config_dict)
    else:
        my_materialized_view = PostgresMaterializedViewRelation.from_dict(config_dict)
        assert my_materialized_view.name == config_dict.get("name")
        assert my_materialized_view.schema_name == config_dict.get("schema").get("name")
        assert my_materialized_view.database_name == config_dict.get("schema").get("database").get(
            "name"
        )
        assert my_materialized_view.query == config_dict.get("query")
        if indexes := config_dict.get("indexes"):
            parsed = {(index.method, index.unique) for index in my_materialized_view.indexes}
            raw = {(index.get("method"), index.get("unique")) for index in indexes}
            assert parsed == raw


def test_create_materialized_view_changeset(materialized_view_relation):
    existing_materialized_view = replace(materialized_view_relation)

    # pulled from `./dbt_postgres_tests/conftest.py`
    original_index_1 = PostgresIndexRelation.from_dict(
        {
            "name": "index_1",
            "column_names": frozenset({"id", "value"}),
            "method": "hash",
            "unique": False,
        }
    )
    original_index_2 = PostgresIndexRelation.from_dict(
        {"name": "index_2", "column_names": frozenset({"id"}), "method": "btree", "unique": True},
    )

    new_index = PostgresIndexRelation.from_dict(
        {"column_names": frozenset({"id", "value"}), "method": "btree", "unique": False}
    )

    target_materialized_view = replace(
        existing_materialized_view, indexes=frozenset({new_index, original_index_2})
    )

    changeset = PostgresMaterializedViewRelationChangeset.from_relations(
        existing_materialized_view, target_materialized_view
    )
    assert changeset.is_empty is False
    assert changeset.requires_full_refresh is False
    assert changeset.indexes == {
        PostgresIndexRelationChange(action=RelationChangeAction.drop, context=original_index_1),
        PostgresIndexRelationChange(action=RelationChangeAction.create, context=new_index),
    }
