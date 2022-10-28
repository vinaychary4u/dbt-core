from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.runnable import GraphRunnableTask, ManifestTask
from dbt.task.test import TestSelector


class LineageTask(GraphRunnableTask):
    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException("manifest and graph must be set to get perform node selection")
        if self.resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
            )
        else:
            return ResourceTypeSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
                resource_types=self.resource_types,
            )

    def run(self):
        ManifestTask._runtime_initialize(self)
        breakpoint()
        pass
