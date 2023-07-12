from dataclasses import replace
from typing import Type

import pytest

from dbt.adapters.relation.models import (
    MaterializedViewRelation,
    MaterializedViewRelationChangeset,
)
from dbt.exceptions import DbtRuntimeError


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
                "my_name": "my_materialized_view",
                "schema": {
                    "name": "my_schema",
                    "database": {"name": "my_database"},
                },
                # missing `query`
            },
            DbtRuntimeError,
        ),
    ],
)
def test_create_materialized_view(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            MaterializedViewRelation.from_dict(config_dict)
    else:
        my_materialized_view = MaterializedViewRelation.from_dict(config_dict)
        assert my_materialized_view.name == config_dict.get("name")
        assert my_materialized_view.schema_name == config_dict.get("schema").get("name")
        assert my_materialized_view.database_name == config_dict.get("schema").get("database").get(
            "name"
        )
        assert my_materialized_view.query == config_dict.get("query")


def test_create_materialized_view_changeset(materialized_view_relation):
    existing_materialized_view = replace(materialized_view_relation)
    target_materialized_view = replace(existing_materialized_view)

    changeset = MaterializedViewRelationChangeset.from_relations(
        existing_materialized_view, target_materialized_view
    )
    assert changeset.is_empty is False
    # the default is a full refresh, even with no change
    assert changeset.requires_full_refresh is True
