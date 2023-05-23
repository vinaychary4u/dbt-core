from copy import deepcopy

import pytest

from dbt.tests.util import relation_from_name
from tests.adapter.dbt.tests.adapter.materialized_view.base import Base
from tests.adapter.dbt.tests.adapter.materialized_view.on_configuration_change import (
    OnConfigurationChangeBase,
    get_project_config,
    set_project_config,
)


class PostgresBasicBase(Base):
    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(materialized='table') }}
        select 1 as base_column
        """
        base_materialized_view = """
        {{ config(materialized='materialized_view') }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_materialized_view.sql": base_materialized_view}


class PostgresOnConfigurationChangeBase(OnConfigurationChangeBase):
    @pytest.fixture(scope="class")
    def models(self):
        base_table = """
        {{ config(
            materialized='table',
            indexes=[{'columns': ['id', 'value']}]
        ) }}
        select
            1 as id,
            100 as value,
            42 as new_id,
            4242 as new_value
        """
        base_materialized_view = """
        {{ config(materialized='materialized_view') }}
        select * from {{ ref('base_table') }}
        """
        return {"base_table.sql": base_table, "base_materialized_view.sql": base_materialized_view}

    @pytest.fixture(scope="function")
    def configuration_changes(self, project):
        initial_config = get_project_config(project)

        # change the index from `id` to `value`
        new_config = deepcopy(initial_config)
        new_config["models"].update({"indexes": [{"columns": ["new_id", "new_value"]}]})
        set_project_config(project, new_config)

        yield

        # set this back for the next test
        set_project_config(project, initial_config)

    @pytest.fixture(scope="function")
    def update_index_message(self, project):
        return f"Applying UPDATE INDEXES to: {relation_from_name(project.adapter, 'base_materialized_view')}"
