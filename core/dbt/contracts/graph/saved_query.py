from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

from dbt.contracts.graph.model_config import SavedQueryConfig
from dbt.contracts.graph.nodes import GraphNode, SourceFileMetadata, WhereFilter


@dataclass
class SavedQuery(GraphNode):
    metrics: List[str]
    group_bys: List[str]
    where: List[WhereFilter]
    description: Optional[str] = None
    label: Optional[str] = None
    metadata: Optional[SourceFileMetadata] = None
    config: SavedQueryConfig = field(default_factory=SavedQueryConfig)
    unrendered_config: Dict[str, Any] = field(default_factory=dict)
