from dbt.interal_deprecations import deprecated


def test_should_warn():
    @deprecated
    def to_be_decorated():
        pass
    to_be_decorated()
