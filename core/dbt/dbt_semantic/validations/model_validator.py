from concurrent.futures import ProcessPoolExecutor, as_completed
import copy
import logging
from typing import List, Sequence
from dbt.exceptions import DbtSemanticValidationError

from dbt.dbt_semantic.objects.user_configured_model import UserConfiguredModel
# from metricflow.model.validations.agg_time_dimension import AggregationTimeDimensionRule
# from metricflow.model.validations.data_sources import DataSourceTimeDimensionWarningsRule, DataSourceValidityWindowRule
# from metricflow.model.validations.dimension_const import DimensionConsistencyRule
# from metricflow.model.validations.element_const import ElementConsistencyRule
# from metricflow.model.validations.identifiers import (
#     IdentifierConfigRule,
#     IdentifierConsistencyRule,
#     NaturalIdentifierConfigurationRule,
#     OnePrimaryIdentifierPerDataSourceRule,
# )
# from metricflow.model.validations.materializations import ValidMaterializationRule
# from metricflow.model.validations.measures import (
#     PercentileAggregationRule,
#     CountAggregationExprRule,
#     DataSourceMeasuresUniqueRule,
#     MeasureConstraintAliasesRule,
#     MetricMeasuresRule,
#     MeasuresNonAdditiveDimensionRule,
# )
# from metricflow.model.validations.metrics import CumulativeMetricRule, DerivedMetricRule
# from metricflow.model.validations.non_empty import NonEmptyRule
from dbt.dbt_semantic.validations.reserved_keywords import ReservedKeywordsRule
from dbt.dbt_semantic.validations.unique_valid_name import UniqueAndValidNameRule
from dbt.dbt_semantic.validations.validator_helpers import (
    ModelValidationResults,
    ModelValidationRule,
    ModelValidationException,
    ModelBuildResult,
)

logger = logging.getLogger(__name__)


class ModelValidator:
    """A Validator that acts on UserConfiguredModel"""

    DEFAULT_RULES = (
        # PercentileAggregationRule(),
        # DerivedMetricRule(),
        # CountAggregationExprRule(),
        # DataSourceMeasuresUniqueRule(),
        # DataSourceTimeDimensionWarningsRule(),
        # DataSourceValidityWindowRule(),
        # DimensionConsistencyRule(),
        # ElementConsistencyRule(),
        # IdentifierConfigRule(),
        # IdentifierConsistencyRule(),
        # NaturalIdentifierConfigurationRule(),
        # OnePrimaryIdentifierPerDataSourceRule(),
        # MeasureConstraintAliasesRule(),
        # MetricMeasuresRule(),
        # CumulativeMetricRule(),
        # NonEmptyRule(),
        UniqueAndValidNameRule(),
        # ValidMaterializationRule(),
        # AggregationTimeDimensionRule(),
        ReservedKeywordsRule(),
        # MeasuresNonAdditiveDimensionRule(),
    )

    def __init__(self, rules: Sequence[ModelValidationRule] = DEFAULT_RULES, max_workers: int = 1) -> None:
        """Constructor.

        Args:
            rules: List of validation rules to run. Defaults to DEFAULT_RULES
            max_workers: sets the max number of rules to run against the model concurrently
        """

        # Raises an error if 'rules' is an empty sequence or None
        if not rules:
            raise ValueError("ModelValidator 'rules' must be a sequence with at least one ModelValidationRule.")

        self._rules = rules
        self._executor = ProcessPoolExecutor(max_workers=max_workers)

    def validate_model(self, model: UserConfiguredModel) -> ModelBuildResult:
        """Validate a model according to configured rules."""

        issues: List[ModelValidationResults] = []

        for rule in self._rules:
            issues.append(ModelValidationResults.from_issues_sequence(rule.validate_model(model)))

        return ModelBuildResult(model=model, issues=ModelValidationResults.merge(issues))

    def checked_validations(self, model: UserConfiguredModel) -> UserConfiguredModel:  # chTODO: remember checked_build
        """Similar to validate(), but throws an exception if validation fails."""
        model_copy = copy.deepcopy(model)
        build_result = self.validate_model(model_copy)

        if build_result.issues.has_blocking_issues:
            raise ModelValidationException(issues=tuple(build_result.issues.all_issues))

        return model


def _print_issues(
    issues: ModelValidationResults, show_non_blocking: bool = False, verbose: bool = False
) -> None:  # noqa: D
    for issue in issues.errors:
        print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")
    if show_non_blocking:
        for issue in issues.future_errors:
            print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")
        for issue in issues.warnings:
            print(f"• {issue.as_cli_formatted_str(verbose=verbose)}")