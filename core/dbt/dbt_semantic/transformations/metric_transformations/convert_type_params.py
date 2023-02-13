from typing import Union, List
from abc import ABC
from dbt.dbt_semantic.objects.metrics import (
    MetricInputMeasure, 
    MetricTimeWindow, 
    MetricInput, 
    UnparsedMetricTypeParams,
    MetricTypeParams
)

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
        if parameters:
            for parameter in parameters:
                if isinstance(parameter,str):
                    parameters_list.append(MetricInputMeasure(name=parameter))
                elif isinstance(parameter,MetricInputMeasure):
                    parameters_list.append(MetricInputMeasure(name=parameter))
            return parameters_list
        else:
            return []

    @staticmethod
    def _get_window_parameter(parameter: Union[str,MetricTimeWindow]):
        if isinstance(parameter,str):
            return MetricTimeWindow.parse(window=parameter)
        elif isinstance(parameter,MetricTimeWindow):
            return parameter

    @staticmethod
    def _get_metric_parameters(parameters: List[Union[MetricInput,str]]):
        parameters_list=[]
        if parameters:
            for parameter in parameters:
                if isinstance(parameter,str):
                    parameters_list.append(MetricInput(name=parameter))
                elif isinstance(parameter,MetricInput):
                    parameters_list.append(MetricInput(name=parameter))
            return parameters_list
        else:
            return []

    @staticmethod
    def _get_metric_type_params(type_params: UnparsedMetricTypeParams) -> MetricTypeParams:

        parsed_type_params=MetricTypeParams(
            measure=ConvertTypeParams._get_parameter(type_params.measure),
            measures=ConvertTypeParams._get_parameters(type_params.measures),
            numerator=ConvertTypeParams._get_parameter(type_params.numerator),
            denominator=ConvertTypeParams._get_parameter(type_params.denominator),
            expr=type_params.expr,
            window=ConvertTypeParams._get_window_parameter(type_params.window),
            grain_to_date=type_params.grain_to_date,
            metrics=ConvertTypeParams._get_metric_parameters(type_params.metrics),
        )

        return parsed_type_params