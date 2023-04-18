from typing import Optional, List, Dict, Any
from datetime import datetime
from dbt.dataclass_schema import dbtClassMixin

from dataclasses import dataclass, field

from dbt.contracts.util import BaseArtifactMetadata, ArtifactMixin, schema_version
from dbt.contracts.graph.unparsed import NodeVersion
from dbt.contracts.graph.nodes import ManifestOrPublicNode
from dbt.node_types import NodeType, AccessType


@dataclass
class DependentProjects(dbtClassMixin):
    name: str
    environment: str


@dataclass
class Dependencies(dbtClassMixin):
    projects: List[DependentProjects] = field(default_factory=list)


@dataclass
class PublicationMetadata(BaseArtifactMetadata):
    dbt_schema_version: str = field(
        default_factory=lambda: str(PublicationArtifact.dbt_schema_version)
    )
    adapter_type: Optional[str] = None
    quoting: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PublicModel(dbtClassMixin, ManifestOrPublicNode):
    name: str
    package_name: str
    unique_id: str
    relation_name: str
    version: Optional[NodeVersion] = None
    is_latest_version: bool = False
    # list of model unique_ids
    public_dependencies: List[str] = field(default_factory=list)
    generated_at: datetime = field(default_factory=datetime.utcnow)

    # Needed for ref resolution code
    @property
    def resource_type(self):
        return NodeType.Model

    # Needed for ref resolution code
    @property
    def access(self):
        return AccessType.Public

    @property
    def search_name(self):
        if self.version is None:
            return self.name
        else:
            return f"{self.name}.v{self.version}"

    @property
    def depends_on_nodes(self):
        return []

    @property
    def depends_on_public_nodes(self):
        return []


@dataclass
class PublicationMandatory:
    project_name: str


@dataclass
@schema_version("publication", 1)
class PublicationArtifact(ArtifactMixin, PublicationMandatory):
    public_models: Dict[str, PublicModel] = field(default_factory=dict)
    metadata: PublicationMetadata = field(default_factory=PublicationMetadata)
    # list of project name strings
    dependencies: List[str] = field(default_factory=list)


@dataclass
class PublicationConfig(ArtifactMixin, PublicationMandatory):
    metadata: PublicationMetadata = field(default_factory=PublicationMetadata)
    # list of project name strings
    dependencies: List[str] = field(default_factory=list)
    public_model_ids: List[str] = field(default_factory=list)
