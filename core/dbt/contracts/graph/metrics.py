from dbt.node_types import NodeType


class MetricReference(object):
    def __init__(self, metric_name, package_name=None):
        self.metric_name = metric_name
        self.package_name = package_name

    def __str__(self):
        return f"{self.metric_name}"