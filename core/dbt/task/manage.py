# coding=utf-8
from typing import Dict, Set, Tuple, AbstractSet
from .compile import CompileTask
from dbt.exceptions import (
    warn_or_error,
)
from dbt.parser.manifest import ManifestLoader
from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
)


class ManageTask(CompileTask):
    def before_run(self, adapter, selected_uids: AbstractSet[str]):
        required_schemas = self.get_model_schemas(adapter, selected_uids)
        self.populate_adapter_cache(adapter, required_schemas)

    def run(self):
        manifest = ManifestLoader.get_full_manifest(self.config)

        managed_schemas_actions_config: Dict[Tuple[str, str], str] = {
            (ms.database or "", ms.schema or ""): ms.action or "warn"
            for ms in self.config.managed_schemas
        }

        if len(managed_schemas_actions_config) == 0:
            warn_or_error(
                "Schema management enabled for connection but no schemas configured to manage"
            )
            return

        # TODO consider compilation before management to enforce valid models
        # was_successfull_complete_run = not any(
        #     r.status in (NodeStatus.Error, NodeStatus.Fail, NodeStatus.Skipped) for r in results
        # )
        # if not was_successfull_complete_run:
        #     warn("One or more models failed, skipping schema management")
        #     return

        models_in_results: Set[Tuple[str, str, str]] = set(
            (n.database, n.schema, n.name)
            for n in manifest.nodes.values()
            if isinstance(n, ParsedModelNode)
        )

        adapter = get_adapter(self.config)
        with adapter.connection_named("master"):
            for database, schema in managed_schemas_actions_config.keys():
                available_models: Dict[Tuple[str, str, str], str] = {
                    (database, schema, relation.identifier): relation
                    for relation in adapter.list_relations(database, schema)
                }
                if len(available_models) == 0:
                    warn_or_error(
                        f"No modules in managed schema '{schema}' for database '{database}'"
                    )
                should_act_upon = available_models.keys() - models_in_results
                for (target_database, target_schema, target_identifier) in should_act_upon:
                    target_action = managed_schemas_actions_config[
                        (target_database, target_schema)
                    ]
                    if target_action == "warn":
                        print("WARN ABOUT ", target_database, target_schema, target_identifier)
                    elif target_action == "drop":
                        adapter.drop_relation(
                            available_models[(target_database, target_schema, target_identifier)]
                        )
