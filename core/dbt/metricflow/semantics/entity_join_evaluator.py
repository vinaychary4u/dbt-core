from __future__ import annotations

from dataclasses import dataclass
from typing import List, Dict, Optional

from dbt.dbt_semantic.objects.identifiers import IdentifierType
from dbt.dbt_semantic.object_utils import pformat_big_objects
from dbt.dbt_semantic.references import IdentifierReference

# from metricflow.instances import DataSourceReference, DataSourceElementReference, IdentifierInstance, InstanceSet
# from metricflow.protocols.semantics import DataSourceSemanticsAccessor
