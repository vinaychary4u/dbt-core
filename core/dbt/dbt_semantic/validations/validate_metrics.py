from abc import ABC 
from dbt.contracts.graph.nodes import Metric


class ValidateMetric(ABC):

    @staticmethod
    def _validate_cumulative_sum_metric_params(metric: Metric)
    print("hello")