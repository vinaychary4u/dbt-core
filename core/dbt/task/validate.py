# This task is intended to validate a semantic layer
from dbt.graph import ResourceTypeSelector
from dbt.task.runnable import GraphRunnableTask, ManifestTask
from dbt.task.test import TestSelector
from dbt.node_types import NodeType
from dbt.events.types import ParseCmdOut
from dbt.exceptions import DbtInternalError
from dbt.events.functions import fire_event
from dbt.semantic.model import UserConfiguredSemanticModel


class ValidateTask(GraphRunnableTask):

    def defer_to_manifest(self, adapter, selected_uids):
        # validate don't defer
        return

    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("Manifest and graph must be set to get perform node selection")
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
        fire_event(ParseCmdOut(msg="Starting validation."))
        ManifestTask._runtime_initialize(self)
        manifest = self.manifest

        model = UserConfiguredSemanticModel(
            entities=manifest.entities,
            metrics=manifest.metrics
        )

        breakpoint()
        print(model)
        #{ 
        # NOTE: Compile is needed for anything dag related, validate to ensure semantic completeness

        # COMPILE LOGIC:
        # 1. DONE: Create the entity() lookup that validates that the entity exists
            # 1a. DONE. Add the BaseEntityResolver in providers
            # 1b. DONE: Add the ResolvedEntityReference a la metrics in graph folder
            # 1c. DONE: Make sure that the entity() lookup outputs to depends_on
        # 1b. DONE: Update the metric() lookup for the new metricflow spec
        # 2. DONE: Add a warning if entity field doesn't use lookup 
        # 3. DONE IN MAIN:Fix exposure logic. Referencing a metric that didn't exist raised an error
        #    for depending on a "source" which not what we want.
        # 3a. DONE: Fix writing to entities in exposures. Unlike refs or metrics, the list of 
        #       lists for entities didn't work
        # 4. ??

        # DOCS LOGIC:
        # 1. DONE: Add entities to docs
        # 2. Fix the issue with the lineage graph 

        # VALIDATE LOGIC:
        # 1. Add the logic that establishes identifier relationships in entities
        # 1a. This logic should be bi-directional? IE should be present in both entities
        # 2. Figure out what the hell is going on with identifiers. 
        # 3. Import and initiate the ModelClass in validate
        # 4. Determine boundaries between validate and compile
        # 
        # 
        # 
        # 
        # }
        fire_event(ParseCmdOut(msg="Callum still has more todos"))

        ## TODO: Warehouse validation? I don't think so?


        # TODO: Import and initiate ModelClass that mimics MF
        # TODO: Perform semantic validation
        # TODO: Have great error messages if validation fails


        fire_event(ParseCmdOut(msg="Validation completed!"))
        fire_event(ParseCmdOut(msg="Done."))

