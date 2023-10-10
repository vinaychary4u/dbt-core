import pytest
from dbt.tests.util import run_dbt, get_manifest, write_file

from dbt.exceptions import ParsingError

from tests.functional.partial_parsing.fixtures import (
    orders_sql,
    orders_downstream_sql,
    groups_schema_yml_one_group,
    groups_schema_yml_two_groups,
    groups_schema_yml_two_groups_edited,
    groups_schema_yml_one_group_model_in_group2,
    groups_schema_yml_two_groups_private_orders_valid_access,
    groups_schema_yml_two_groups_private_orders_invalid_access,
)


class TestGroups:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "orders.sql": orders_sql,
            "orders_downstream.sql": orders_downstream_sql,
            "schema.yml": groups_schema_yml_one_group,
        }

    def test_pp_groups(self, project):

        # initial run
        results = run_dbt()
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # add group to schema
        write_file(groups_schema_yml_two_groups, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group", "group.test.test_group2"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # edit group in schema
        write_file(
            groups_schema_yml_two_groups_edited, project.project_root, "models", "schema.yml"
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group", "group.test.test_group2_edited"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # delete group in schema
        write_file(groups_schema_yml_one_group, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2
        manifest = get_manifest(project.project_root)
        expected_nodes = ["model.test.orders", "model.test.orders_downstream"]
        expected_groups = ["group.test.test_group"]
        assert expected_nodes == sorted(list(manifest.nodes.keys()))
        assert expected_groups == sorted(list(manifest.groups.keys()))

        # add back second group
        write_file(groups_schema_yml_two_groups, project.project_root, "models", "schema.yml")
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        # remove second group with model still configured to second group
        write_file(
            groups_schema_yml_one_group_model_in_group2,
            project.project_root,
            "models",
            "schema.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])

        # add back second group, make orders private with valid ref
        write_file(
            groups_schema_yml_two_groups_private_orders_valid_access,
            project.project_root,
            "models",
            "schema.yml",
        )
        results = run_dbt(["--partial-parse", "run"])
        assert len(results) == 2

        write_file(
            groups_schema_yml_two_groups_private_orders_invalid_access,
            project.project_root,
            "models",
            "schema.yml",
        )
        with pytest.raises(ParsingError):
            results = run_dbt(["--partial-parse", "run"])
