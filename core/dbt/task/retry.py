from pathlib import Path
from typing import Dict, Optional, Type

from dbt import selected_resources
from dbt.cli.flags import Flags
from dbt.cli.types import Command, CombinedResultType
from dbt.config import RuntimeConfig
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


RETRYABLE_TASKS: Dict[Command, Type[BaseTask]] = {
    Command.BUILD: BuildTask,
    Command.COMPILE: CompileTask,
    Command.DOCS_GENERATE: GenerateTask,
    Command.RUN: RunTask,
    Command.SEED: SeedTask,
    Command.SHOW: ShowTask,
    Command.SNAPSHOT: SnapshotTask,
    Command.SOURCE_FRESHNESS: FreshnessTask,
    Command.TEST: TestTask,
}


class RetryTask:
    task_class: Optional[Type[BaseTask]]

    def __init__(self, flags, config, manifest, previous_command, previous_state):
        self.flags = flags
        self.config = config
        self.manifest = manifest
        self.previous_command = previous_command
        self.previous_state = previous_state

    def run(self) -> CombinedResultType:
        self.task_class = RETRYABLE_TASKS.get(self.previous_command, None)
        if self.task_class is None:
            raise DbtInternalError(f"Command '{self.previous_command.value}' does not support retry")

        statuses_to_skip = [NodeStatus.Success, NodeStatus.Pass, NodeStatus.Warn]
        unique_ids = set(
            [
                result.unique_id
                for result in self.previous_state.results.results
                if result.status not in statuses_to_skip
            ]
        )

        # TODO: remove this line when #6009 is resolved
        selected_resources.set_selected_resources(unique_ids)

        class TaskWrapper(self.task_class):  # type: ignore
            def original_compile_manifest(self):
                super().compile_manifest()

            def compile_manifest(self):
                pass

            def get_graph_queue(self):
                new_graph = self.graph.get_subset_graph(unique_ids)
                return GraphQueue(
                    new_graph.graph,
                    self.manifest,
                    unique_ids,
                )

        task = TaskWrapper(
            self.flags,
            self.config,
            self.manifest,
        )

        task.original_compile_manifest()

        return task.run()

    def interpret_results(self, *args, **kwargs):
        if self.task_class is None:
            raise DbtInternalError("Can't interpret results without a task class set on RetryTask")

        return self.task_class.interpret_results(*args, **kwargs)
