import traceback
from typing import List

from dbt.exceptions import DbtSemanticValidationError
from dbt.dbt_semantic.references import MetricModelReference
from dbt.contracts.graph.nodes import Metric
from dbt.contracts.graph.metrics import MetricType, MetricTimeWindow
from dbt.contracts.graph.manifest import UserConfiguredModel
from dbt.dbt_semantic.validations.unique_valid_name import UniqueAndValidNameRule
from dbt.dbt_semantic.validations.validator_helpers import (
    MetricContext,
    ModelValidationRule,
    ValidationIssueType,
    ValidationError,
)


class CumulativeMetricRule(ModelValidationRule):
    """Checks that cumulative sum metrics are configured properly"""

    @staticmethod
    def _validate_cumulative_sum_metric_params(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        if metric.type == MetricType.CUMULATIVE:
            if metric.type_params.window and metric.type_params.grain_to_date:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message="Both window and grain_to_date set for cumulative metric. Please set one or the other",
                    )
                )

            if metric.type_params.window:
                try:
                    MetricTimeWindow.parse(metric.type_params.window.to_string())
                except DbtSemanticValidationError as e:
                    issues.append(
                        ValidationError(
                            context=MetricContext(
                                metric=MetricModelReference(metric_name=metric.name),
                            ),
                            message="".join(traceback.format_exception_only(etype=type(e), value=e)),
                            extra_detail="".join(traceback.format_tb(e.__traceback__)),
                        )
                    )

        return issues

    @staticmethod
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        for metric in model.metrics or []:
            issues += CumulativeMetricRule._validate_cumulative_sum_metric_params(metric=metric)

        return issues


class DerivedMetricRule(ModelValidationRule):
    """Checks that derived metrics are configured properly"""

    @staticmethod
    def _validate_alias_collision(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        if metric.type == MetricType.DERIVED:
            metric_context = MetricContext(
                metric=MetricModelReference(metric_name=metric.name),
            )
            used_names = {input_metric.name for input_metric in metric.input_metrics}
            for input_metric in metric.input_metrics:
                if input_metric.alias:
                    issues += UniqueAndValidNameRule.check_valid_name(input_metric.alias, metric_context)
                    if input_metric.alias in used_names:
                        issues.append(
                            ValidationError(
                                message=f"Alias '{input_metric.alias}' for input metric: '{input_metric.name}' is already being used. Please choose another alias.",
                            )
                        )
                        used_names.add(input_metric.alias)
        return issues

    @staticmethod
    def _validate_input_metrics_exist(model: UserConfiguredModel) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        all_metrics = {m.name for m in model.metrics}
        for metric in model.metrics:
            if metric.type == MetricType.DERIVED:
                for input_metric in metric.input_metrics:
                    if input_metric.name not in all_metrics:
                        issues.append(
                            ValidationError(
                                context=MetricContext(
                                    metric=MetricModelReference(metric_name=metric.name),
                                ),
                                message=f"For metric: {metric.name}, input metric: '{input_metric.name}' does not exist as a configured metric in the model.",
                            )
                        )
        return issues

    @staticmethod
    def _validate_time_offset_params(metric: Metric) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        for input_metric in metric.input_metrics or []:
            if input_metric.offset_window and input_metric.offset_to_grain:
                issues.append(
                    ValidationError(
                        context=MetricContext(
                            metric=MetricModelReference(metric_name=metric.name),
                        ),
                        message=f"Both offset_window and offset_to_grain set for derived metric '{metric.name}' on input metric '{input_metric.name}'. Please set one or the other.",
                    )
                )

        return issues

    @staticmethod
    def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        issues: List[ValidationIssueType] = []

        issues += DerivedMetricRule._validate_input_metrics_exist(model=model)
        for metric in model.metrics or []:
            issues += DerivedMetricRule._validate_alias_collision(metric=metric)
            issues += DerivedMetricRule._validate_time_offset_params(metric=metric)
        return issues