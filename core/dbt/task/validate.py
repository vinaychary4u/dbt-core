# This task is intended to validate a semantic layer
from dbt.graph import ResourceTypeSelector
from dbt.task.runnable import GraphRunnableTask, ManifestTask
from dbt.task.test import TestSelector
from dbt.node_types import NodeType
from dbt.events.types import ParseCmdOut
from dbt.exceptions import DbtInternalError
from dbt.events.functions import fire_event
from dbt.semantic.user_configured_model import UserConfiguredModel


class ValidateTask(ManifestTask):

    def run(self):
        fire_event(ParseCmdOut(msg="Starting validation."))

        ManifestTask.load_manifest(self)

        model=self.manifest.user_configured_model
        breakpoint()
        fire_event(ParseCmdOut(msg="Callum still has more todos"))

        fire_event(ParseCmdOut(msg="Validation completed!"))
        fire_event(ParseCmdOut(msg="Done."))

