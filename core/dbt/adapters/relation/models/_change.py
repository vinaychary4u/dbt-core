from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from typing import Any, Dict, Hashable

from dbt.dataclass_schema import StrEnum
from dbt.exceptions import DbtRuntimeError
from dbt.utils import filter_null_values

from dbt.adapters.relation.models._relation import Relation


class RelationChangeAction(StrEnum):
    alter = "alter"
    create = "create"
    drop = "drop"


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RelationChange(ABC):
    """
    Changes are generally "alter the thing in place" or "drop the old one in favor of the new one". In other words,
    you will either wind up with a single `alter` or a pair of `drop` and `create`. In the `alter` scenario,
    `context` tends to be a single value, like a setting. In the `drop` and `create` scenario,
    `context` tends to be the whole object, in particular for `create`.
    """

    action: StrEnum  # this will generally be `RelationChangeAction`, however this allows for extending that Enum
    context: Hashable  # this is usually a RelationConfig, e.g. `IndexConfig`, or single value, e.g. `str`

    @property
    @abstractmethod
    def requires_full_refresh(self) -> bool:
        """
        Indicates if this change can be performed via alter logic (hence `False`), or will require a full refresh
        (hence `True`). While this is generally determined by the type of change being made, which could be a
        static property, this is purposely being left as a dynamic property to allow for edge cases.

        Returns:
            `True` if the change requires a full refresh, `False` if the change can be applied to the object
        """
        raise NotImplementedError(
            "Configuration change management has not been fully configured for this adapter and/or relation type."
        )


@dataclass
class RelationChangeset(ABC):
    existing_relation: Relation
    target_relation: Relation
    _requires_full_refresh_override: bool = False

    def __post_init__(self):
        if self.is_empty and self.existing_relation != self.target_relation:
            # we need to force a full refresh if we didn't detect any changes but the objects are not the same
            self.force_full_refresh()

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "RelationChangeset":
        kwargs_dict = deepcopy(config_dict)
        try:
            return cls(**filter_null_values(kwargs_dict))
        except TypeError:
            raise DbtRuntimeError(f"Unexpected configuration received:\n" f"    {config_dict}\n")

    @classmethod
    def from_relations(
        cls, existing_relation: Relation, target_relation: Relation
    ) -> "RelationChangeset":
        kwargs_dict = cls.parse_relations(existing_relation, target_relation)

        # stuff the relations in so that we can do the post init to figure out if we need a full refresh
        kwargs_dict.update(
            {
                "existing_relation": existing_relation,
                "target_relation": target_relation,
            }
        )
        return cls.from_dict(kwargs_dict)

    @classmethod
    @abstractmethod
    def parse_relations(
        cls, existing_relation: Relation, target_relation: Relation
    ) -> Dict[str, Any]:
        raise NotImplementedError(
            "Configuration change management has not been fully configured for this adapter and/or relation type."
        )

    @property
    def requires_full_refresh(self) -> bool:
        """
        This should be a calculation based on the changes that you stack on this class.
        Remember to call `super().requires_full_refresh()` in your conditions, or at least reference
        `self._requires_full_refresh_override`

        Returns:
            `True` if any change requires a full refresh or if the override has been triggered
            `False` if all changes can be made without requiring a full refresh
        """
        return self._requires_full_refresh_override

    @property
    def is_empty(self) -> bool:
        """
        Indicates if there are any changes in this changeset.

        Returns:
            `True` if there is any change or if the override has been triggered
            `False` if there are no changes
        """
        return not self._requires_full_refresh_override

    def force_full_refresh(self):
        """
        Activates the full refresh override.
        """
        self._requires_full_refresh_override = True
