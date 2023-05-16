import pytest

from dbt.tests.util import run_dbt
from dbt.contracts.results import RunStatus
from tests.functional.retry.fixtures import (
    model_one,
    model_two,
    model_three,
    model_four,
    seed_one,
    snapshot_one,
    snapshot_two,
)


class TestRetryModels:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one,
            "model_two.sql": model_two,
            "model_three.sql": model_three,
            "model_four.sql": model_four,
        }

    def test_build(self, project):
        self._test_runnable("build")

    def test_compile(self, project):
        # compilation still succeeds on nodes that would fail on execution
        compile_result = run_dbt(["compile", "--target-path", "state"])
        assert len(compile_result) == 4
        assert compile_result[0].status == RunStatus.Success
        assert compile_result[1].status == RunStatus.Success
        assert compile_result[2].status == RunStatus.Success
        assert compile_result[3].status == RunStatus.Success

        # all nodes have already been compiled
        retry_result = run_dbt(["retry", "--state", "state"], add_dirs=False)
        assert len(retry_result) == 0

    def test_docs_generate(self, project):
        # docs generate only operates on successful nodes
        gen_result = run_dbt(["docs", "generate", "--target-path", "state"])
        assert len(gen_result.nodes) == 2
        assert "model.test.model_one" in gen_result.nodes
        assert "model.test.model_two" in gen_result.nodes

        retry_result = run_dbt(["retry", "--state", "state"], add_dirs=False)
        assert len(retry_result.nodes) == 2
        assert "model.test.model_one" in gen_result.nodes
        assert "model.test.model_two" in gen_result.nodes

    def test_run(self, project):
        self._test_runnable("run")

    def test_source_freshness(self, project):
        pass

    def _test_runnable(self, command):
        # "runnable" commands include failed nodes in their results
        command_result = run_dbt(
            [command, "--target-path", "state"],
            expect_pass=False,
        )
        assert len(command_result) == 4
        assert command_result[0].status == RunStatus.Success
        assert command_result[1].status == RunStatus.Success
        assert command_result[2].status == RunStatus.Error
        assert command_result[3].status == RunStatus.Skipped

        # model one and two succeeded, so they do not require rerunning
        retry_result = run_dbt(
            ["retry", "--state", "state"],
            add_dirs=False,
            expect_pass=False,
        )
        assert len(retry_result) == 2
        assert retry_result[0].node.name == "model_three"
        assert retry_result[0].status == RunStatus.Error
        assert retry_result[1].node.name == "model_four"
        assert retry_result[1].status == RunStatus.Skipped


class TestRetrySeeds:
    @pytest.fixture(scope="class")
    def seeds(self):
        return {"seed_one.csv": seed_one}

    def test_seed(self, project):
        # once the only seed has been run, there are no more seeds to retry
        seed_result = run_dbt(["seed", "--target-path", "state"])
        assert len(seed_result) == 1

        retry_result = run_dbt(["retry", "--state", "state"], add_dirs=False)
        assert len(retry_result) == 0


class TestRetrySnapshots:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one,
            "model_two.sql": model_two,
            "model_three.sql": model_three,
        }

    @pytest.fixture(scope="class")
    def snapshots(self):
        return {
            "snapshot_one.sql": snapshot_one,
            "snapshot_two.sql": snapshot_two,
        }

    def test_snapshot(self, project):
        snapshot_result = run_dbt(
            ["snapshot", "--target-path", "state"],
            expect_pass=False,
        )
        assert len(snapshot_result) == 2
        assert snapshot_result[0].node.name == "snapshot_one"
        assert snapshot_result[0].status == RunStatus.Success
        assert snapshot_result[1].node.name == "snapshot_two"
        assert snapshot_result[1].status == RunStatus.Error

        retry_result = run_dbt(
            ["retry", "--state", "state"],
            expect_pass=False,
            add_dirs=False,
        )
        assert len(retry_result) == 1
        assert retry_result[0].node.name == "snapshot_two"
        assert retry_result[0].status == RunStatus.Error