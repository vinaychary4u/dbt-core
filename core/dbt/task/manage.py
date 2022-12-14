# coding=utf-8
from typing import Dict, Set, Tuple, List

from .runnable import ManifestTask, GraphRunnableTask
from dbt.exceptions import warn_or_error, ValidationException
from dbt.adapters.factory import get_adapter
from dbt.contracts.graph.parsed import (
    ParsedModelNode,
)
from dbt.contracts.project import PruneModelsAction
from dbt.utils import executor
from concurrent.futures import as_completed, Future
from dbt.adapters.base import BaseRelation


class ManageTask(ManifestTask):
    def _runtime_initialize(self):
        # we just need to load the manifest
        # we don't actually need to 'compile' it into a DAG/queue, if we're not doing node selection
        self.load_manifest()

    def before_run(self, adapter):
        with adapter.connection_named("master"):
            # this is a bad look! but we just want to borrow this one method
            # populating the cache will speed up calls to 'list_relations' further down
            # in all other ways, ManageTask is much more like a ManifestTask than a GraphRunnableTask
            # (e.g. it doesn't need a DAG queue)
            GraphRunnableTask.populate_adapter_cache(self, adapter)

    def run(self):
        self._runtime_initialize()
        adapter = get_adapter(self.config)
        self.before_run(adapter)
        try:
            with adapter.connection_named("master"):
                adapter.clear_transaction()
                self._prune_models(adapter)
        finally:
            adapter.cleanup_connections()

    def _prune_models(self, adapter):
        # TODO: do this uniqueness check as part of config validation (= parsing),
        # instead of during task/runtime
        self._assert_schema_uniqueness()

        if len(self.config.managed_schemas) == 0:
            # TODO: turn into structured event
            warn_or_error("No schemas configured to manage")
            return

        models_in_codebase: Set[Tuple[Tuple[str, str, str]]] = set(
            (
                # what the user has configured - use this one for matching
                (n.config.database, n.config.schema),
                # the resolved values - actual location in database - use this one for lookup
                (n.database, n.schema, n.alias),
            )
            for n in self.manifest.nodes.values()
            if isinstance(n, ParsedModelNode)  # TODO: seeds? tests via --store-failures?
        )

        should_drop = set()
        should_warn = set()

        for config in self.config.managed_schemas:
            target_action = config.prune_models or PruneModelsAction.SKIP

            # if set to 'skip', let's skip right away!
            if target_action == PruneModelsAction.SKIP:
                continue

            # these are the 'database' and 'schema' configs set in dbt_project.yml
            managed_db_config = config.database
            managed_schema_config = config.schema

            # THE PLAN
            # match models from the manifest that have the same configured values of 'database' + 'schema'
            # then, look up their resolved 'database' + 'schema'.
            # Depending on custom behavior of generate_X_name macros,
            # this relationship (configured -> resolved) could be one -> one, one -> many, or many -> one

            resolved_schemas_to_check = set()

            models = [
                model
                for model in models_in_codebase
                if (
                    (
                        model[0][0] == managed_db_config
                        or (model[0][0] is None and managed_db_config is None)
                    )
                    and (
                        model[0][1] == managed_schema_config
                        or (model[0][1] is None and managed_schema_config is None)
                    )
                )
            ]
            if len(models) == 0:
                # TODO: turn into structured event
                warn_or_error(
                    f"While managing configured schema '{managed_db_config}.{managed_schema_config}': No models found with matching config"
                )
            for model in models:
                # now store the resolved 'database' + 'schema' values of matched nodes
                resolved_schemas_to_check.add((model[1][0], model[1][1]))

            # It's also possible that the last model from a 'managed schema' was just deleted or disabled.
            # For thoroughness, we'll also resolve the naive (not-node-specific) result of 'generate_X_name' macros, and check those too.
            generated_database_name = adapter.execute_macro(
                "generate_database_name", kwargs={"custom_database_name": config.database}
            )
            generated_schema_name = adapter.execute_macro(
                "generate_schema_name", kwargs={"custom_schema_name": config.schema}
            )
            resolved_schemas_to_check.add((generated_database_name, generated_schema_name))

            # It's possible that we've already checked this schema via another managed_schema config (the many -> one case)
            # but we still want to tag the resolved objects with the managed_schema action, in case it's 'warn' in one case and 'drop' in another
            # in any case, these calls to 'list_relations' should be cache hits
            for database, schema in resolved_schemas_to_check:
                models_in_database: Dict[Tuple[str, str, str], str] = {
                    (database, schema, relation.identifier): relation
                    for relation in adapter.list_relations(database, schema)
                }
                if len(models_in_database) == 0:
                    # TODO: turn into structured event
                    warn_or_error(
                        f"No objects in managed schema '{database}.{schema}', resolved from config '{managed_db_config}.{managed_schema_config}'"
                    )

                # compare 'found' models_in_database against 'expected' values from manifest
                expected_models = set([model[1] for model in models_in_codebase])
                should_act_upon = models_in_database.keys() - expected_models

                for (target_database, target_schema, target_identifier) in sorted(should_act_upon):
                    object = models_in_database[
                        (target_database, target_schema, target_identifier)
                    ]
                    if target_action == PruneModelsAction.WARN:
                        should_warn.add(object)
                    elif target_action == PruneModelsAction.DROP:
                        should_drop.add(object)

        # If the same object appeared in both the 'warn' and 'drop' categories,
        # let's err on the safe side: warn, don't drop.
        # This can happen due to custom behavior in generate_X_name macros, such that
        # the relationship from configured -> resolved is many -> one,
        # and the same object will be found in different 'managed_schemas' with different configured actions.
        # Example pattern: `generate_schema_name_for_env` with `target.name != prod`. All custom schemas are ignored, and every model just lands in {target.schema}.
        to_drop = should_drop - should_warn

        # First, warn about everything configured to warn
        for object in should_warn:
            # TODO: turn into structured event
            message = f"Found unused object {object}"
            if object in should_drop:
                message = message + ", not dropping because also configured to warn"
            warn_or_error(message)

        # Now, drop everything that needs dropping
        # with concurrency up to number of configured --threads
        with executor(self.config) as tpe:
            futures: List[Future[List[BaseRelation]]] = []
            for object in to_drop:
                # TODO: turn into structured event
                print(f"Dropping {object}")
                fut = tpe.submit_connected(
                    adapter,
                    f"drop_{object.database}_{object.schema}",
                    adapter.drop_relation,
                    object,
                )
                futures.append(fut)

            for fut in as_completed(futures):
                # trigger/re-raise any exceptions while dropping relations
                fut.result()

    def _assert_schema_uniqueness(self):
        schemas = set()

        for config in self.config.managed_schemas:
            schema = (config.database, config.schema)
            if schema in schemas:
                raise ValidationException(f"Duplicate schema found: {schema}")
            schemas.add(schema)

    def interpret_results(self, results):
        # if all queries succeed, this will return exit code 0 ("success")
        # if any query fails, or warning + --warn-error, it will return exit code 2 ("unhandled")
        return True
