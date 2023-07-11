from abc import ABC
from collections import OrderedDict
from dataclasses import dataclass

from dbt.contracts.relation import ComponentName, RelationType

from dbt.adapters.relation.models._relation_component import RelationComponent
from dbt.adapters.relation.models._schema import SchemaRelation


@dataclass(frozen=True)
class Relation(RelationComponent, ABC):

    # attribution
    name: str
    schema: SchemaRelation

    """
    TODO: `can_be_renamed` belongs on `Relation`; however, I get the error below and cannot figure out how to fix it.

        TypeError: non-default argument 'can_be_renamed' follows default argument

    """
    # configuration
    type: RelationType
    SchemaParser: SchemaRelation

    def __str__(self) -> str:
        return self.fully_qualified_path

    @property
    def fully_qualified_path(self) -> str:
        return self.render.full(
            OrderedDict(
                {
                    ComponentName.Database: self.database_name,
                    ComponentName.Schema: self.schema_name,
                    ComponentName.Identifier: self.name,
                }
            )
        )

    @property
    def schema_name(self) -> str:
        return self.schema.name

    @property
    def database_name(self) -> str:
        return self.schema.database_name

    @classmethod
    def from_dict(cls, config_dict) -> "Relation":
        """
        Parse `config_dict` into a `MaterializationViewRelation` instance, applying defaults
        """
        # default configuration
        kwargs_dict = {"SchemaParser": cls.SchemaParser}
        kwargs_dict.update(config_dict)

        if schema := config_dict.get("schema"):
            kwargs_dict.update({"schema": kwargs_dict["SchemaParser"].from_dict(schema)})

        relation = super().from_dict(kwargs_dict)
        assert isinstance(relation, Relation)
        return relation
