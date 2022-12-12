import pytest

from dbt.internal_deprecations import deprecated
from dbt.exceptions import (
    CompilationException,
    raise_compiler_error,
    DatabaseException,
    raise_database_error,
)


def is_decorated(func):
    return hasattr(func, '__wrapped__')


@deprecated(reason="just because", version="1.23.0", suggested_action="Make some updates")
def to_be_decorated():
    return 5


def test_deprecated():
    assert(is_decorated(to_be_decorated))
    # This should not alter the return value of the function
    assert(to_be_decorated() == 5)


def test_deprecated_functions():
    assert(is_decorated(raise_compiler_error))
    with pytest.raises(CompilationException):
        raise_compiler_error(msg="Exception message")

    assert(is_decorated(raise_database_error))
    with pytest.raises(DatabaseException):  # not currently deprecated - how to add check?
        raise_database_error(msg="Exception message")
