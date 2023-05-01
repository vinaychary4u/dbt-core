from distutils.util import strtobool

from dataclasses import dataclass
from dbt.utils import _coerce_decimal
from dbt.events.format import pluralize
from dbt.dataclass_schema import dbtClassMixin
import threading
from typing import Dict, Any, Optional
import io

from .compile import CompileRunner
from .run import RunTask

from dbt.contracts.graph.nodes import (
    TestNode,
)
from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import TestStatus, PrimitiveDict, RunResult
from dbt.context.providers import generate_runtime_model_context
from dbt.clients.jinja import MacroGenerator
from dbt.events.functions import fire_event
from dbt.events.types import (
    LogTestResult,
    LogStartLine,
)
from dbt.exceptions import (
    DbtInternalError,
    BooleanError,
    MissingMaterializationError,
)
from dbt.graph import (
    ResourceTypeSelector,
)
from dbt.node_types import NodeType
from dbt.flags import get_flags


@dataclass
class TestResultData(dbtClassMixin):
    failures: int
    should_warn: bool
    should_error: bool
    adapter_response: Dict[str, Any]

    @classmethod
    def validate(cls, data):
        data["should_warn"] = cls.convert_bool_type(data["should_warn"])
        data["should_error"] = cls.convert_bool_type(data["should_error"])
        super().validate(data)

    def convert_bool_type(field) -> bool:
        # if it's type string let python decide if it's a valid value to convert to bool
        if isinstance(field, str):
            try:
                return bool(strtobool(field))  # type: ignore
            except ValueError:
                raise BooleanError(field, "get_test_sql")

        # need this so we catch both true bools and 0/1
        return bool(field)


@dataclass
class UnitTestResultData(TestResultData):
    diff: Optional[str] = None
    should_error: bool
    adapter_response: Dict[str, Any]


class TestRunner(CompileRunner):
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

    def execute_test(self, test: TestNode, manifest: Manifest) -> TestResultData:
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
        if test.resource_type == NodeType.Unit:
            actual = self._get_unit_test_table(table, "actual")
            expected = self._get_unit_test_table(table, "expected")
            should_error = actual.rows != expected.rows
            failures = 1 if should_error else 0
            diff = None
            if should_error:
                actual_output = self._agate_table_to_str(actual)
                expected_output = self._agate_table_to_str(expected)

                diff = f"\n\nActual:\n{actual_output}\nExpected:\n{expected_output}"
            return UnitTestResultData(
                diff=diff,
                should_error=should_error,
                should_warn=False,
                failures=failures,
                adapter_response=adapter_response,
            )
        else:
            num_rows = len(table.rows)
            if num_rows != 1:
                raise DbtInternalError(
                    f"dbt internally failed to execute {test.unique_id}: "
                    f"Returned {num_rows} rows, but expected "
                    f"1 row"
                )
            num_cols = len(table.columns)
            if num_cols != 3:
                raise DbtInternalError(
                    f"dbt internally failed to execute {test.unique_id}: "
                    f"Returned {num_cols} columns, but expected "
                    f"3 columns"
                )

            test_result_dct: PrimitiveDict = dict(
                zip(
                    [column_name.lower() for column_name in table.column_names],
                    map(_coerce_decimal, table.rows[0]),
                )
            )
            test_result_dct["adapter_response"] = adapter_response
            TestResultData.validate(test_result_dct)
            return TestResultData.from_dict(test_result_dct)

    def execute(self, test: TestNode, manifest: Manifest):
        result = self.execute_test(test, manifest)
        thread_id = threading.current_thread().name
        num_errors = pluralize(result.failures, "result")

        status = TestStatus.Pass
        message = None
        failures = 0
        if isinstance(result, UnitTestResultData):
            if result.should_error:
                status = TestStatus.Fail
                message = result.diff
                failures = result.failures
        else:
            severity = test.config.severity.upper()
            if severity == "ERROR" and result.should_error:
                status = TestStatus.Fail
                message = f"Got {num_errors}, configured to fail if {test.config.error_if}"
                failures = result.failures
            elif result.should_warn:
                if get_flags().WARN_ERROR:
                    status = TestStatus.Fail
                    message = f"Got {num_errors}, configured to fail if {test.config.warn_if}"
                else:
                    status = TestStatus.Warn
                    message = f"Got {num_errors}, configured to warn if {test.config.warn_if}"
                failures = result.failures

        return RunResult(
            node=test,
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


class TestSelector(ResourceTypeSelector):
    def __init__(self, graph, manifest, previous_state):
        super().__init__(
            graph=graph,
            manifest=manifest,
            previous_state=previous_state,
            resource_types=[NodeType.Test, NodeType.Unit],
        )


class TestTask(RunTask):
    """
    Testing:
        Read schema files + custom data tests and validate that
        constraints are satisfied.
    """

    __test__ = False

    def raise_on_first_error(self):
        return False

    def get_node_selector(self) -> TestSelector:
        if self.manifest is None or self.graph is None:
            raise DbtInternalError("manifest and graph must be set to get perform node selection")
        return TestSelector(
            graph=self.graph,
            manifest=self.manifest,
            previous_state=self.previous_state,
        )

    def get_runner_type(self, _):
        return TestRunner
