# coding=utf-8
from typing import Dict, Set, Tuple
from .compile import CompileTask
from .runnable import ManifestTask
from dbt.exceptions import (
    warn_or_error,
)
from dbt.parser.manifest import ManifestLoader
from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
)
from dbt.contracts.project import PruneModelsAction


class ManageTask(CompileTask):
    def run(self):
        ManifestTask._runtime_initialize(self)
        selected_uids = self.manifest.nodes.keys()
        adapter = get_adapter(self.config)

        with adapter.connection_named("master"):
            required_schemas = self.get_model_schemas(adapter, selected_uids)
            self.populate_adapter_cache(adapter, required_schemas)
            # TODO: defer to manifest?

            adapter.clear_transaction()
            self.manage_schema(adapter)

    def manage_schema(self, adapter):
        manifest = ManifestLoader.get_full_manifest(self.config)

        # Read config
        managed_schemas_actions_config: Dict[Tuple[str, str], str] = {
            (ms.database or "", ms.schema or ""): ms.prune_models or PruneModelsAction.SKIP
            for ms in self.config.managed_schemas
        }

        if len(managed_schemas_actions_config) == 0:
            warn_or_error(
                "Schema management enabled for connection but no schema's configured to manage"
            )
            return

        models_in_results: Set[Tuple[str, str, str]] = set(
            (n.database, n.schema, n.name)
            for n in manifest.nodes.values()
            if isinstance(n, ParsedModelNode)
        )

        for database, schema in managed_schemas_actions_config.keys():
            available_models: Dict[Tuple[str, str, str], str] = {
                (database, schema, relation.identifier): relation
                for relation in adapter.list_relations(database, schema)
            }

            if len(available_models) == 0:
                warn_or_error(f"No modules in managed schema '{schema}' for database '{database}'")
            should_act_upon = available_models.keys() - models_in_results
            for (target_database, target_schema, target_identifier) in should_act_upon:
                target_action = managed_schemas_actions_config[(target_database, target_schema)]
                if target_action == PruneModelsAction.WARN:
                    warn_or_error(
                        f"Found unused model {target_database}.{target_schema}.{target_identifier}"
                    )
                elif target_action == PruneModelsAction.DROP:
                    adapter.drop_relation(
                        available_models[(target_database, target_schema, target_identifier)]
                    )

    def interpret_results(self, results):
        return True
