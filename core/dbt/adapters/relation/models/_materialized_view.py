from abc import ABC
from dataclasses import dataclass

from dbt.contracts.relation import RelationType
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.relation.models._change import RelationChangeset
from dbt.adapters.relation.models._relation import Relation


@dataclass(frozen=True)
class MaterializedViewRelation(Relation, ABC):
    """
    This config identifies the minimal materialization parameters required for dbt to function as well
    as built-ins that make macros more extensible. Additional parameters may be added by subclassing for your adapter.
    """

    # attribution
    query: str

    # configuration
    # TODO: move `can_be_renamed` to `Relation`; see `Relation` for more information
    can_be_renamed: bool

    @classmethod
    def from_dict(cls, config_dict) -> "MaterializedViewRelation":
        """
        Parse `config_dict` into a `MaterializationViewRelation` instance, applying defaults
        """
        # default configuration
        kwargs_dict = {
            "type": RelationType.MaterializedView,
            "can_be_renamed": cls.can_be_renamed,
        }
        kwargs_dict.update(config_dict)

        materialized_view = super().from_dict(kwargs_dict)
        assert isinstance(materialized_view, MaterializedViewRelation)
        return materialized_view


class MaterializedViewRelationChangeset(RelationChangeset):
    @classmethod
    def parse_relations(cls, existing_relation: Relation, target_relation: Relation) -> dict:
        try:
            assert isinstance(existing_relation, MaterializedViewRelation)
            assert isinstance(target_relation, MaterializedViewRelation)
        except AssertionError:
            raise DbtRuntimeError(
                f"Two materialized view relations were expected, but received:\n"
                f"    existing: {existing_relation}\n"
                f"    new: {target_relation}\n"
            )

        return {}
