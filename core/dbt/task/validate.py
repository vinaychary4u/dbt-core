# This task is intended to validate a semantic layer
from dbt.graph import ResourceTypeSelector
from dbt.task.runnable import GraphRunnableTask
from dbt.task.test import TestSelector
from dbt.node_types import NodeType
from dbt.events.types import DebugCmdOut
from dbt.exceptions import DbtInternalError
from dbt.events.functions import fire_event
from dbt.semantic.user_configured_model import UserConfiguredModel


class ValidateTask(GraphRunnableTask):
    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)
        if self.args.models:
            if self.args.select:
                raise DbtRuntimeError('"models" and "select" are mutually exclusive arguments')
            if self.args.resource_types:
                raise DbtRuntimeError(
                    '"models" and "resource_type" are mutually exclusive ' "arguments"
                )

    def run(self):
        fire_event(DebugCmdOut(msg="Starting validation."))

        GraphRunnableTask.load_manifest(self)

        model = self.manifest.user_configured_model
        breakpoint()
        fire_event(DebugCmdOut(msg="Callum still has more todos"))

        fire_event(DebugCmdOut(msg="Validation completed!"))
        fire_event(DebugCmdOut(msg="Done."))
