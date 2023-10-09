from dataclasses import dataclass
from typing import List, Optional

from dbt.contracts.graph.nodes import GraphNode, SourceFileMetadata, WhereFilter


@dataclass
class SavedQuery(GraphNode):
    metrics: List[str]
    group_bys: List[str]
    where: List[WhereFilter]
    description: Optional[str] = None
    label: Optional[str] = None
    metadata: Optional[SourceFileMetadata] = None
