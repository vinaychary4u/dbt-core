from concurrent.futures import ProcessPoolExecutor
import copy
from typing import List, Sequence

from dbt.contracts.graph.manifest import UserConfiguredModel
from dbt.dbt_semantic.validations.agg_time_dimensions import AggregationTimeDimensionRule
from dbt.dbt_semantic.validations.entities import EntityTimeDimensionWarningsRule, EntityValidityWindowRule
from dbt.dbt_semantic.validations.dimension_const import DimensionConsistencyRule
from dbt.dbt_semantic.validations.element_const import ElementConsistencyRule
from dbt.dbt_semantic.validations.identifiers import (
    IdentifierConfigRule,
    IdentifierConsistencyRule,
    NaturalIdentifierConfigurationRule,
    OnePrimaryIdentifierPerEntityRule,
)
from dbt.dbt_semantic.validations.measures import (
    CountAggregationExprRule,
    EntityMeasuresUniqueRule,
    MeasureConstraintAliasesRule,
    MetricMeasuresRule,
    MeasuresNonAdditiveDimensionRule,
)
from dbt.dbt_semantic.validations.metrics import CumulativeMetricRule, DerivedMetricRule
from dbt.dbt_semantic.validations.reserved_keywords import ReservedKeywordsRule
from dbt.dbt_semantic.validations.unique_valid_name import UniqueAndValidNameRule
from dbt.dbt_semantic.validations.validator_helpers import (
    ModelValidationResults,
    ModelValidationRule,
    ModelValidationException,
    ModelBuildResult,
)


class ModelValidator:
    """A Validator that acts on UserConfiguredModel"""

    DEFAULT_RULES = (
        DerivedMetricRule(),
        CountAggregationExprRule(),
        EntityMeasuresUniqueRule(),
        EntityTimeDimensionWarningsRule(),
        EntityValidityWindowRule(),
        DimensionConsistencyRule(),
        ElementConsistencyRule(),
        IdentifierConfigRule(),
        IdentifierConsistencyRule(),
        NaturalIdentifierConfigurationRule(),
        OnePrimaryIdentifierPerEntityRule(),
        MeasureConstraintAliasesRule(),
        MetricMeasuresRule(),
        CumulativeMetricRule(),
        UniqueAndValidNameRule(),
        AggregationTimeDimensionRule(),
        ReservedKeywordsRule(),
        MeasuresNonAdditiveDimensionRule(),
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