from typing import Set, List
from abc import ABC
from dbt.exceptions import DbtSemanticValidationError
from dbt.contracts.graph.metrics import MetricType, MetricInputMeasure
from dbt.contracts.graph.nodes import Metric


class AddInputMetricMeasures(ABC):
    """Add all measures corresponding to the input metrics of the derived metric."""

    @staticmethod
    def _get_measures_for_metric(metric_name: str, metrics: List[Metric]) -> Set[MetricInputMeasure]:
        """Returns a unique set of input measures for a given metric."""
        measures = set()
        metrics_generator=(metric for metric in metrics if metric.name == metric_name)
        matched_metric = next(iter(metrics_generator), None)
        if matched_metric:
            if matched_metric.type == MetricType.DERIVED:
                for input_metric in matched_metric.input_metrics:
                    measures.update(AddInputMetricMeasures._get_measures_for_metric(input_metric.name, metrics))
            else:
                measures.update(set(matched_metric.input_measures))
        else:
            raise DbtSemanticValidationError(f"Metric '{metric_name}' is not configured as a metric in the model.")
        return measures

    @staticmethod
    def add_input_metrics(metric:Metric, metrics: List[Metric]) -> Metric:  # noqa: D
        if metric.type == MetricType.DERIVED:
            measures = AddInputMetricMeasures._get_measures_for_metric(metric.name,metrics)
            if metric.type_params.measures is None:
                raise DbtSemanticValidationError(f"Metric '{metric.name}' is derived, which cannot have measures predefined in config.")
            metric.type_params.measures = list(measures)
        return metric