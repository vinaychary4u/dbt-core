from typing import Union, List
from abc import ABC
from dbt.contracts.graph.unparsed import MetricInputMeasure, MetricTimeWindow, MetricInput
from dbt.contracts.graph.nodes import MetricTypeParams

class ConvertTypeParams(ABC):
    """All the functionality needed to convert UnparsedMetricTypeParams to MetricTypeParams"""

    @staticmethod
    def _get_parameter(parameter: Union[str,MetricInputMeasure]):
        if isinstance(parameter,str):
            return MetricInputMeasure(name=parameter)
        elif isinstance(parameter,MetricInputMeasure):
            return parameter

    @staticmethod
    def _get_parameters(parameters: List[Union[MetricInputMeasure,str]]):
        parameters_list=[]
        for parameter in parameters:
            if isinstance(parameter,str):
                parameters_list.append(MetricInputMeasure(name=parameter))
            elif isinstance(parameter,MetricInputMeasure):
                parameters_list.append(MetricInputMeasure(name=parameter))
        return parameters_list

    @staticmethod
    def _get_window_parameter(parameter: Union[str,MetricTimeWindow]):
        if isinstance(parameter,str):
            return MetricTimeWindow.parse(window=parameter)
        elif isinstance(parameter,MetricTimeWindow):
            return parameter

    @staticmethod
    def _get_metric_parameters(parameters: List[Union[MetricInput,str]]):
        parameters_list=[]
        for parameter in parameters:
            if isinstance(parameter,str):
                parameters_list.append(MetricInput(name=parameter))
            elif isinstance(parameter,MetricInput):
                parameters_list.append(MetricInput(name=parameter))
        return parameters_list
