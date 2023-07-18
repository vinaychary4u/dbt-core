from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Hashable

from dbt.adapters.relation.models._relation import RelationComponent
from dbt.dataclass_schema import StrEnum


class RelationChangeAction(StrEnum):
    alter = "alter"
    create = "create"
    drop = "drop"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RelationChange(RelationComponent, ABC):
    action: RelationChangeAction
    context: Hashable  # this is usually a RelationConfig, e.g. IndexConfig, but shouldn't be limited

    @property
    @abstractmethod
    def requires_full_refresh(self) -> bool:
        raise self._not_implemented_error()
