from dbt.contracts.results import RunExecutionResult, RunResultsArtifact


def execute(run_result: RunExecutionResult, path: str):

    RunResultsArtifact.from_execution_results(
        results=run_result.results,
        elapsed_time=run_result.elapsed_time,
        generated_at=run_result.generated_at,
        args=run_result.args,
    ).write(path)
