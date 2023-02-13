from dataclasses import dataclass, field
from dbt.dataclass_schema import dbtClassMixin, StrEnum, ExtensibleDbtClassMixin, ValidationError
from dbt.contracts.graph.nodes import Entity, Metric
from typing import List


@dataclass
class UserConfiguredModel(dbtClassMixin):
    """Model holds all the information the SemanticLayer needs to render a query"""

    entities: List[Entity] = field(default_factory=list)
    metrics: List[Metric] = field(default_factory=list)