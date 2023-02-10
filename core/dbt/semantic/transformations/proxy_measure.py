from dbt.contracts.graph.unparsed import (
    UnparsedMetric,
    MetricType, 
    MetricTypeParams, 
    MetricInputMeasure
)
from dbt.node_types import NodeType
from dbt.contracts.graph.nodes import (
    Metric
)

from dbt.context.providers import generate_parse_metrics

from dbt.exceptions import DbtValidationError


def create_proxy_metric(parsed_entity,measure,package_name,path,unique_id,fqn,original_file_path,meta,tags,config,unrendered_config):    
    
    #TODO: Bring this more in line with MetricParser
    add_metric = True
    # These ensures that the unique id and fqn are correct for the metric and not the entity
    unique_id = unique_id.replace('entity','metric')
    unique_id = unique_id.replace(fqn[2],measure.name)
    fqn[2] = measure.name

    for metric in parsed_entity.metrics:
        if metric.name == measure.name:
            if metric.type != MetricType.MEASURE_PROXY:
                raise DbtValidationError(
                    f"Cannot have metric with the same name as a measure ({measure.name}) that is not a "
                    f"proxy for that measure"
                )
            add_metric = False
    
    if add_metric:
        proxy_metric = Metric(
            resource_type=NodeType.Metric,
            package_name=package_name,
            path=path,
            original_file_path=original_file_path,
            unique_id=unique_id,
            fqn=fqn,
            name=measure.name,
            entity=parsed_entity.name,
            description=measure.description,
            type=MetricType.MEASURE_PROXY,
            type_params=
            MetricTypeParams(
                # TODO: Get MetricInputMeasure working
                measure=measure.name,
                expression=measure.name,
            ),
            meta=meta,
            tags=tags,
            config=config,
            unrendered_config=unrendered_config
        )
             
        return proxy_metric
