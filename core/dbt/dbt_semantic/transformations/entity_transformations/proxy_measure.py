from dbt.dbt_semantic.objects.metrics import (
    MetricType, 
    MetricInputMeasure,
    MetricTypeParams
)
from dbt.node_types import NodeType
from dbt.contracts.graph.nodes import (
    Metric,
    Entity
)
from dbt.clients.jinja import get_rendered
from abc import ABC
from typing import List
from dbt.context.providers import (
    generate_parse_metrics,
)

from dbt.exceptions import DbtValidationError

class ProxyMeasure(ABC):
    """All the functionality needed to convert measures to metrics"""

    def _create_proxy_metrics(self, parsed_entity: Entity, path: str, fqn: List):    
        if parsed_entity.measures:
            for measure in parsed_entity.measures:
                if measure.create_metric:
                    add_metric = True
                    package_name = self.project.project_name
                    unique_id = f"{NodeType.Metric}.{package_name}.{measure.name}"
                    original_file_path=self.yaml.path.original_file_path
                    fqn[2] = measure.name

                    for metric in parsed_entity.metrics:
                        if metric.name == measure.name:
                            if metric.type != MetricType.MEASURE_PROXY:
                                raise DbtValidationError(
                                    f"Cannot have metric with the same name as a measure ({measure.name}) that is not a "
                                    f"proxy for that measure"
                                )
                            add_metric = False

                    config=self._generate_proxy_metric_config(
                        target=measure,
                        fqn=fqn,
                        package_name=package_name,
                        rendered=True,
                    )

                    config = config.finalize_and_validate()

                    unrendered_config = self._generate_proxy_metric_config(
                        target=measure,
                        fqn=fqn,
                        package_name=package_name,
                        rendered=False,
                    )

                    if measure.expr:
                        measure_expr=measure.expr
                    else: 
                        measure_expr=measure.name

                    if add_metric:
                        proxy_metric = Metric(
                            resource_type=NodeType.Metric,
                            package_name=package_name,
                            path=path,
                            original_file_path=original_file_path,
                            unique_id=unique_id,
                            fqn=fqn,
                            name=measure.name,
                            entity="entity('" + parsed_entity.name + "')",
                            description=measure.description,
                            type=MetricType.MEASURE_PROXY,
                            type_params=MetricTypeParams(
                                measure=MetricInputMeasure(name=measure.name),
                                expr=measure_expr,
                            ),
                            meta=measure.meta,
                            tags=measure.tags,
                            config=config,
                            unrendered_config=unrendered_config
                        )
                
                        proxy_ctx = generate_parse_metrics(
                            proxy_metric,
                            self.root_project,
                            self.schema_parser.manifest,
                            package_name
                        )

                        if proxy_metric.entity is not None:
                            entity_ref = "{{ " + proxy_metric.entity + " }}"
                            get_rendered(entity_ref, proxy_ctx, proxy_metric)

                        if proxy_metric.config.enabled:
                            self.manifest.add_metric(self.yaml.file, proxy_metric)
                        else:
                            self.manifest.add_disabled(self.yaml.file, proxy_metric) 
            
        return self