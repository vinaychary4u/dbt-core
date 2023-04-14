from typing import Optional, List, Dict, Any
from dbt.dataclass_schema import dbtClassMixin

from dataclasses import dataclass, field

from dbt.contracts.util import BaseArtifactMetadata, ArtifactMixin, schema_version


@dataclass
class DependentProjects(dbtClassMixin):
    name: str
    environment: str


@dataclass
class Dependencies(dbtClassMixin):
    projects: list[DependentProjects] = field(default_factory=list)


@dataclass
class PublicationMetadata(BaseArtifactMetadata):
    dbt_schema_version: str = field(default_factory=lambda: str(Publication.dbt_schema_version))
    adapter_type: Optional[str] = None
    quoting: Dict[str, Any] = field(default_factory=dict)


@dataclass
class PublicModel(dbtClassMixin):
    relation_name: str
    is_latest_version: bool = False
    # list of model unique_ids
    public_dependencies: List[str] = field(default_factory=list)


@dataclass
class PublicationMandatory:
    project_name: str


@dataclass
@schema_version("publication", 1)
class Publication(ArtifactMixin, PublicationMandatory):
    public_models: Dict[str, PublicModel] = field(default_factory=dict)
    metadata: PublicationMetadata = field(default_factory=PublicationMetadata)
    # list of project name strings
    dependencies: List[str] = field(default_factory=list)
