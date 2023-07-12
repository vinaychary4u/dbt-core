from typing import Type

import pytest
from dbt.exceptions import DbtRuntimeError

from dbt.adapters.relation.models import DatabaseRelation


@pytest.mark.parametrize(
    "config_dict,exception",
    [
        ({"name": "my_database"}, None),
        # there are no validation rules, so "" is a valid name for the default
        ({"name": ""}, None),
        ({"wrong_name": "my_database"}, DbtRuntimeError),
        ({}, DbtRuntimeError),
    ],
)
def test_make_database(config_dict: dict, exception: Type[Exception]):
    if exception:
        with pytest.raises(exception):
            DatabaseRelation.from_dict(config_dict)
    else:
        my_database = DatabaseRelation.from_dict(config_dict)
        assert my_database.name == config_dict.get("name")
