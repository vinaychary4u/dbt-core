from copy import deepcopy

import pytest

from dbt.tests.util import relation_from_name
from tests.adapter.dbt.tests.adapter.materialized_view.base import Model
from tests.adapter.dbt.tests.adapter.materialized_view.on_configuration_change import (
    OnConfigurationChangeBase,
    get_project_config,
    set_project_config,
)


_BASE_TABLE = """
{{ config(
    materialized='table',
    indexes=[{'columns': ['id'], 'type': 'hash'}]
) }}
select
    1 as id,
    100 as value
where 0 = 1
"""


class PostgresOnConfigurationChangeBase(OnConfigurationChangeBase):

    base_table = Model(name="base_table", definition=_BASE_TABLE, columns=["id", "value"])

    starting_records = [(1, 100)]
    inserted_records = [(2, 200)]

    @pytest.fixture(scope="function")
    def configuration_changes(self, project):
        initial_config = get_project_config(project)

        # change the index from `id` to `value`
        new_config = deepcopy(initial_config)
        new_config["models"].update({"indexes": [{"columns": ["value"], "type": "hash"}]})
        set_project_config(project, new_config)

        yield

        # set this back for the next test
        set_project_config(project, initial_config)

    @pytest.fixture(scope="function")
    def update_index_message(self, project):
        return f"Applying UPDATE INDEXES to: {relation_from_name(project.adapter, self.base_materialized_view.name)}"
