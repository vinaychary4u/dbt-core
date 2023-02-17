#TODO: Delete this rule as we don't need to enforce that these nodes are present



# from typing import List

# from dbt.contracts.graph.manifest import UserConfiguredModel
# from dbt.dbt_semantic.validations.validator_helpers import (
#     ModelValidationRule,
#     ValidationError,
#     ValidationIssueType,
# )


# class NonEmptyRule(ModelValidationRule):
#     """Check if the model contains data sources and metrics."""

#     @staticmethod
#     def _check_model_has_data_sources(model: UserConfiguredModel) -> List[ValidationIssueType]:
#         issues: List[ValidationIssueType] = []
#         if not model.entities:
#             issues.append(
#                 ValidationError(
#                     message="No entities present in the model.",
#                 )
#             )
#         return issues

#     @staticmethod
#     def _check_model_has_metrics(model: UserConfiguredModel) -> List[ValidationIssueType]:
#         issues: List[ValidationIssueType] = []

#         # If we are going to generate measure proxy metrics that is sufficient as well
#         create_measure_proxy_metrics = False
#         for data_source in model.data_sources:
#             for measure in data_source.measures:
#                 if measure.create_metric is True:
#                     create_measure_proxy_metrics = True
#                     break

#         if not model.metrics and not create_measure_proxy_metrics:
#             issues.append(
#                 ValidationError(
#                     message="No metrics present in the model.",
#                 )
#             )
#         return issues

#     @staticmethod
#     def validate_model(model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
#         issues: List[ValidationIssueType] = []
#         issues += NonEmptyRule._check_model_has_data_sources(model=model)
#         issues += NonEmptyRule._check_model_has_metrics(model=model)
#         return issues