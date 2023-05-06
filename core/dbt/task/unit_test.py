from dataclasses import dataclass
from dbt.dataclass_schema import dbtClassMixin
import threading
from typing import Dict, Any, Optional
import io

from .compile import CompileRunner
from .run import RunTask

from dbt.contracts.graph.nodes import ModelNode
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import TestStatus, RunResult
from dbt.context.providers import generate_runtime_model_context
from dbt.clients.jinja import MacroGenerator
from dbt.events.functions import fire_event
from dbt.events.types import (
    LogTestResult,
    LogStartLine,
)
from dbt.graph import ResourceTypeSelector
from dbt.exceptions import (
    DbtInternalError,
    MissingMaterializationError,
)
from dbt.node_types import NodeType


@dataclass
class UnitTestResultData(dbtClassMixin):
    should_error: bool
    adapter_response: Dict[str, Any]
    diff: Optional[str] = None


class UnitTestRunner(CompileRunner):
    def describe_node(self):
        return f"{self.node.resource_type} {self.node.name}"

    def print_result_line(self, result):
        model = result.node

        fire_event(
            LogTestResult(
                name=model.name,
                status=str(result.status),
                index=self.node_index,
                num_models=self.num_nodes,
                execution_time=result.execution_time,
                node_info=model.node_info,
                num_failures=result.failures,
            ),
            level=LogTestResult.status_to_level(str(result.status)),
        )

    def print_start_line(self):
        fire_event(
            LogStartLine(
                description=self.describe_node(),
                index=self.node_index,
                total=self.num_nodes,
                node_info=self.node.node_info,
            )
        )

    def before_execute(self):
        self.print_start_line()

    def execute_unit_test(self, node: ModelNode, manifest: Manifest) -> UnitTestResultData:
        test = node
        context = generate_runtime_model_context(test, self.config, manifest)

        materialization_macro = manifest.find_materialization_macro_by_name(
            self.config.project_name, test.get_materialization(), self.adapter.type()
        )

        if materialization_macro is None:
            raise MissingMaterializationError(
                materialization=test.get_materialization(), adapter_type=self.adapter.type()
            )

        if "config" not in context:
            raise DbtInternalError(
                "Invalid materialization context generated, missing config: {}".format(context)
            )

        # generate materialization macro
        macro_func = MacroGenerator(materialization_macro, context)
        # execute materialization macro
        macro_func()
        # load results from context
        # could eventually be returned directly by materialization
        result = context["load_result"]("main")
        adapter_response = result["response"].to_dict(omit_none=True)
        table = result["table"]
        actual = self._get_unit_test_table(table, "actual")
        expected = self._get_unit_test_table(table, "expected")
        should_error = actual.rows != expected.rows
        diff = None
        if should_error:
            actual_output = self._agate_table_to_str(actual)
            expected_output = self._agate_table_to_str(expected)

            diff = f"\n\nActual:\n{actual_output}\nExpected:\n{expected_output}"
        return UnitTestResultData(
            diff=diff,
            should_error=should_error,
            adapter_response=adapter_response,
        )

    def execute(self, node: ModelNode, manifest: Manifest):
        result = self.execute_unit_test(node, manifest)
        thread_id = threading.current_thread().name

        status = TestStatus.Pass
        message = None
        failures = 0
        if result.should_error:
            status = TestStatus.Fail
            message = result.diff
            failures = 1

        return RunResult(
            node=node,
            status=status,
            timing=[],
            thread_id=thread_id,
            execution_time=0,
            message=message,
            adapter_response=result.adapter_response,
            failures=failures,
        )

    def after_execute(self, result):
        self.print_result_line(result)

    def _get_unit_test_table(self, result_table, actual_or_expected: str):
        unit_test_table = result_table.where(
            lambda row: row["actual_or_expected"] == actual_or_expected
        )
        columns = list(unit_test_table.columns.keys())
        columns.remove("actual_or_expected")
        return unit_test_table.select(columns)

    def _agate_table_to_str(self, table) -> str:
        # Hack to get Agate table output as string
        output = io.StringIO()
        # if self.args.output == "json":
        #     table.to_json(path=output)
        # else:
        table.print_table(output=output, max_rows=None)
        return output.getvalue()


class UnitTestSelector(ResourceTypeSelector):
    def __init__(self, graph, manifest, previous_state):
        super().__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
            resource_types=[NodeType.Unit],
        )


class UnitTestTask(RunTask):
    """
    Unit testing:
        Read schema files + custom data tests and validate that
        constraints are satisfied.
    """

    def __init__(self, args, config, manifest, collection):
        super().__init__(args, config, collection)
        self.collection = collection
        self.original_manifest = manifest

    __test__ = False

    def raise_on_first_error(self):
        return False

    def get_node_selector(self) -> UnitTestSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return UnitTestSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
        )

    def get_runner_type(self, _):
        return UnitTestRunner
