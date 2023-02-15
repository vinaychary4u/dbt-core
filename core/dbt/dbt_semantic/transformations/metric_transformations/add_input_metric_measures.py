from typing import Set, List
from abc import ABC
from dbt.exceptions import DbtSemanticValidationError
from dbt.dbt_semantic.objects.metrics import MetricType, MetricInputMeasure
from dbt.contracts.graph.nodes import Metric


class AddInputMetricMeasures(ABC):
    """Add all measures corresponding to the input metrics of the derived metric."""

    @staticmethod
    def _get_measures_for_metric(metric_name: str, manifest_metrics) -> Set[MetricInputMeasure]:
        """Returns a unique set of input measures for a given metric."""
        measures = set()
        matched_metric = next(iter((metric for metric in manifest_metrics if metric.name == metric_name)), None)
        if matched_metric:
            if matched_metric.type == MetricType.DERIVED:
                for input_metric in matched_metric.input_metrics:
                    measures.update(AddInputMetricMeasures._get_measures_for_metric(manifest_metrics, input_metric.name))
            else:
                measures.update(set(matched_metric.input_measures))
        else:
            raise DbtSemanticValidationError(f"Metric '{metric_name}' is not configured as a metric in the model.")
        return measures

    @staticmethod
    def add_input_metrics(manifest):  # noqa: D
        metrics=manifest.metrics.values()
        for metric in metrics:
            if metric.type == MetricType.DERIVED:
                breakpoint()
                measures = AddInputMetricMeasures._get_measures_for_metric(metric.name,metrics)
                if metric.type_params.measures is None:
                    raise DbtSemanticValidationError(f"Metric '{metric.name}' is derived, which cannot have measures predefined in config.")
                metric.type_params.measures = list(measures)
            return metric