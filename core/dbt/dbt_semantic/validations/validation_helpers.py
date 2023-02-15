from dataclasses import dataclass
from dbt.dbt_semantic.objects.dimensions import DimensionType


@dataclass(frozen=True)
class DimensionInvariants:
    """Helper object to ensure consistent dimension attributes across data sources.
    All dimensions with a given name in all data sources should have attributes matching these values.
    """

    type: DimensionType
    is_partition: bool