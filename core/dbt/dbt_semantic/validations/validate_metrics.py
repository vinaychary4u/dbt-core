from dbt.contracts.graph.nodes import Metric
from dbt.dbt_semantic.objects.metrics import MetricType, MetricTimeWindow
from dbt.dbt_semantic.validations.unique_valid_names import UniqueAndValidNames
from dbt.exceptions import DbtSemanticValidationError


class MetricValidator:
    """This class exists to contain the functions we use to validate metrics.
    It is called in schemas.py after metric parsing to validate them."""

    @staticmethod
    def _validate_cumulative_sum_metric_params(metric: Metric):
        validation_errors = []
        if metric.type_params.window and metric.type_params.grain_to_date:
            validation_errors.append("cannot contain both window and grain_to_date in a cumulative metric")
        if metric.type_params.window:
            try:
                MetricTimeWindow.parse(metric.type_params.window.to_string())
            except:
                validation_errors.append("has an invalid window definition")

        if validation_errors:
            raise DbtSemanticValidationError(
                f"The metric name '{metric.name}' is invalid.  It {', '.join(e for e in validation_errors)}"
            )

    @staticmethod
    def _validate_derived_metric_alias_collision(metric: Metric):
        validation_errors = []
        used_names = {input_metric.name for input_metric in metric.input_metrics}
        for input_metric in metric.input_metrics:
            if input_metric.alias:
                validation_errors += UniqueAndValidNames._check_valid_name(name=input_metric.alias)
                if input_metric.alias in used_names:
                    validation_errors.append(
                        f"Alias '{input_metric.alias}' for input_metric '{input_metric.name}' is already being used."
                    )
                    used_names.add(input_metric.alias)
        
        if validation_errors:
            raise DbtSemanticValidationError(
                f"There are issues with metric aliases. {': '.join(e for e in validation_errors)}"
            )

    @staticmethod
    def _validate_derived_metric_input_metrics(metric: Metric, metric_names):
        validation_errors = []
        for input_metric in metric.input_metrics:
            if input_metric.name not in metric_names:
                validation_errors.append(f"contains the input metric {input_metric.name} which does not exist as a configured metric")
            if input_metric.offset_window and input_metric.offset_to_grain:
                validation_errors.append(f"contains the input metric {input_metric.name} which has both offset_window and offset_to_grain set")

        if validation_errors:
            raise DbtSemanticValidationError(
                f"The metric name '{metric.name}' is invalid.  It {', '.join(e for e in validation_errors)}"
            )

    @staticmethod
    def validate_metric(manifest_metrics):  # noqa: D
        
        metric_names = [metric.name for metric in manifest_metrics]

        for metric in manifest_metrics:
            if metric.type == MetricType.CUMULATIVE:
                MetricValidator._validate_cumulative_sum_metric_params(metric=metric)

            if metric.type == MetricType.DERIVED:
                MetricValidator._validate_derived_metric_alias_collision(metric=metric)
                MetricValidator._validate_derived_metric_input_metrics(metric=metric,metric_names=metric_names)
            