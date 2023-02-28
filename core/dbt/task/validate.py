# This task is intended to validate a semantic layer
from dbt.task.runnable import GraphRunnableTask
from dbt.events.types import DebugCmdOut
from dbt.events.functions import fire_event


class ValidateTask(GraphRunnableTask):
    def __init__(self, args, config, manifest):
        super().__init__(args, config, manifest)

    def run(self):
        fire_event(DebugCmdOut(msg="Starting validation."))

        GraphRunnableTask.load_manifest(self)

        breakpoint()

        fire_event(DebugCmdOut(msg="Callum still has more todos"))

        fire_event(DebugCmdOut(msg="Validation completed!"))
        fire_event(DebugCmdOut(msg="Done."))
