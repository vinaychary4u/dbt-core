import pytest
from dbt.internal_deprecations import deprecated

from dbt.exceptions import raise_compiler_error, CompilationException


def test_should_warn():
    @deprecated(reason="just because", version="1.23.0", suggested_action="Make some updates")
    def to_be_decorated():
        pass
    to_be_decorated()


def test_should_error():
    @deprecated(reason="just because", version="1.23.0", suggested_action="Make some updates")
    def to_be_decorated():
        pass
    to_be_decorated()


def test_deprecated_function():
    with pytest.raises(CompilationException) as exc:
        msg = "Something went wrong"
        raise_compiler_error(msg)
    print(exc)
