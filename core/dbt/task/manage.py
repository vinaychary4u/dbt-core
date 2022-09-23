# coding=utf-8
from typing import Dict, Set, Tuple

from .compile import CompileTask
from .runnable import ManifestTask
from dbt.exceptions import warn_or_error, ValidationException
from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
)
from dbt.contracts.project import PruneModelsAction


class ManageTask(CompileTask):
    def run(self):
        ManifestTask._runtime_initialize(self)
        models_in_codebase = self.manifest.nodes.keys()
        adapter = get_adapter(self.config)

        with adapter.connection_named("master"):
            required_schemas = self.get_model_schemas(adapter, models_in_codebase)
            self.populate_adapter_cache(adapter, required_schemas)

            adapter.clear_transaction()
            self._prune_models(adapter)

    def _prune_models(self, adapter):
        self._assert_schema_uniqueness()

        if len(self.config.managed_schemas) == 0:
            warn_or_error(
                "Schema management enabled for connection but no schema's configured to manage"
            )
            return

        models_in_codebase: Set[Tuple[str, str, str]] = set(
            (n.database, n.schema, n.name)
            for n in self.manifest.nodes.values()
            if isinstance(n, ParsedModelNode)
        )

        for config in self.config.managed_schemas:
            generated_schema_name = adapter.execute_macro(
                "generate_schema_name", kwargs={"custom_schema_name": config.schema}
            )

            models_in_database: Dict[Tuple[str, str, str], str] = {
                (config.database, generated_schema_name, relation.identifier): relation
                for relation in adapter.list_relations(config.database, generated_schema_name)
            }
            if len(models_in_database) == 0:
                warn_or_error(
                    f"No modules in managed schema '{generated_schema_name}' for database '{config.database}'"
                )

            should_act_upon = models_in_database.keys() - models_in_codebase

            for (target_database, target_schema, target_identifier) in should_act_upon:
                target_action = config.prune_models or PruneModelsAction.SKIP
                if target_action == PruneModelsAction.WARN:
                    warn_or_error(
                        f"Found unused model {target_database}.{target_schema}.{target_identifier}"
                    )
                elif target_action == PruneModelsAction.DROP:
                    adapter.drop_relation(
                        models_in_database[(target_database, target_schema, target_identifier)]
                    )

    def _assert_schema_uniqueness(self):
        schemas = set()

        for config in self.config.managed_schemas:
            schema = (config.database, config.schema)
            if schema in schemas:
                raise ValidationException(f"Duplicate schema found: {schema}")
            schemas.add(schema)

    def interpret_results(self, results):
        return True
