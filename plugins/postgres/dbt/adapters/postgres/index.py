from dataclasses import dataclass, asdict
from datetime import datetime
from typing import List, Optional

from hologram import ValidationError

import dbt.utils
from dbt.dataclass_schema import dbtClassMixin
from dbt.exceptions import IndexConfigError, IndexConfigNotDictError


@dataclass
class PostgresIndexConfig(dbtClassMixin):
    columns: List[str]
    unique: bool = False
    type: Optional[str] = "btree"
    name: Optional[str] = None

    def render(self, relation):
        # We append the current timestamp to the index name because otherwise
        # the index will only be created on every other run. See
        # https://github.com/dbt-labs/dbt-core/issues/1945#issuecomment-576714925
        # for an explanation.
        now = datetime.utcnow().isoformat()
        inputs = self.columns + [relation.render(), str(self.unique), str(self.type), now]
        string = "_".join(inputs)
        return dbt.utils.md5(string)

    @classmethod
    def parse(cls, raw_index) -> Optional["PostgresIndexConfig"]:
        if raw_index is None:
            return None
        try:
            cls.validate(raw_index)
            return cls.from_dict(raw_index)
        except ValidationError as exc:
            raise IndexConfigError(exc)
        except TypeError:
            raise IndexConfigNotDictError(raw_index)

    @property
    def as_config_dict(self) -> dict:
        # Boil this back down to a user-submitted config for the postgres create index query
        config = asdict(self)
        config.pop("name")
        return config

    def __hash__(self):
        # Allow for sets of indexes defined only by columns, type, and uniqueness; i.e. remove the timestamp
        return hash((frozenset(x.upper() for x in self.columns), self.type, self.unique))
