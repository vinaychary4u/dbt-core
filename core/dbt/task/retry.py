from typing import Any, Optional

from dbt import selected_resources
from dbt.cli.flags import Flags
from dbt.cli.utils import get_runtime_config
from dbt.config.runtime import RuntimeConfig
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import NodeStatus
from dbt.contracts.state import PreviousState
from dbt.exceptions import DbtInternalError
from dbt.graph import GraphQueue

from dbt.task.base import BaseTask
from dbt.task.build import BuildTask
from dbt.task.compile import CompileTask
from dbt.task.freshness import FreshnessTask
from dbt.task.generate import GenerateTask
from dbt.task.run import RunTask
from dbt.task.seed import SeedTask
from dbt.task.show import ShowTask
from dbt.task.snapshot import SnapshotTask
from dbt.task.test import TestTask


TASK_DICT = {
    "build": BuildTask,
    "compile": CompileTask,
    "freshness": FreshnessTask,
    "generate": GenerateTask,
    "run": RunTask,
    "seed": SeedTask,
    "show": ShowTask,
    "snapshot": SnapshotTask,
    "test": TestTask,
}


class RetryTask:
    task_class: Optional[BaseTask]

    def __init__(self, flags: Flags, config: RuntimeConfig, manifest: Manifest):
        self.flags = flags
        self.config = config
        self.manifest = manifest
    
    def run(self) -> Any: # TODO: type as anything a task can return (look at dbtRunner)
        previous_state = PreviousState(
            self.flags.state,
            self.flags.state,
        )

        cmd_name = previous_state.results.args.get("which")
        if cmd_name == "retry":
            raise DbtInternalError("Can't retry a retry command")

        self.task_class = TASK_DICT.get(cmd_name, None)
        if self.task_class is None:
            raise DbtInternalError(f'No command mapped to string "{cmd_name}"')

        # should this interact with --warn-error?
        statuses_to_skip = [NodeStatus.Success, NodeStatus.Pass, NodeStatus.Warn]
        unique_ids = set([
            result.unique_id
            for result in previous_state.results.results
            if result.status not in statuses_to_skip
        ])

        # does #6009 need to be resolved before this ticket?
        selected_resources.set_selected_resources(unique_ids)

        class TaskWrapper(self.task_class):
            def original_compile_manifest(self):
                return super().compile_manifest()

            def compile_manifest(self):
                pass

            def get_graph_queue(self):
                new_graph = self.graph.get_subset_graph(unique_ids)
                return GraphQueue(
                    new_graph.graph,
                    self.manifest,
                    unique_ids,
                )

        retry_flags = Flags.from_dict_for_cmd(cmd_name, previous_state.results.args)
        retry_config = get_runtime_config(retry_flags)

        task = TaskWrapper(
            retry_flags,
            retry_config,
            self.manifest,
        )

        task.original_compile_manifest()

        return task.run()

    def interpret_results(self, *args, **kwargs):
        return self.task_class.interpret_results(*args, **kwargs)
