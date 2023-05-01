import pytest

from dbt.tests.util import run_dbt
from dbt.contracts.results import RunStatus


model_one = """
    select 1 as fun
"""

model_two = """
    select * from {{ ref("model_one") }}
"""

model_three = """
    breaking line
    select * from {{ ref("model_two") }}
"""

model_four = """
    select * from {{ ref("model_three") }}
"""


class TestRunRetry:
    @pytest.fixture(scope="class")
    def models(self):
        return {
            "model_one.sql": model_one,
            "model_two.sql": model_two,
            "model_three.sql": model_three,
            "model_four.sql": model_four,
        }

    def test_run(self, project):
        run_results = run_dbt(["run", "--target-path", "state"], expect_pass=False)
        assert len(run_results) == 4
        assert run_results[0].status == RunStatus.Success
        assert run_results[1].status == RunStatus.Success
        assert run_results[2].status == RunStatus.Error
        assert run_results[3].status == RunStatus.Skipped

        retry_results = run_dbt(["retry", "--state", "state"], expect_pass=False)
        assert len(retry_results) == 2
        assert retry_results[0].node.name == "model_three"
        assert retry_results[0].status == RunStatus.Error
        assert retry_results[1].node.name == "model_four"
        assert retry_results[1].status == RunStatus.Skipped
