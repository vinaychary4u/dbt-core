# flake8: noqa
import pytest
from dbt.exceptions import Exception



class TestBaseException:

    def test_base_exception(self):
        with pytest.raises(Exception) as exc_info:
            raise(Exception())
        breakpoint()
        print("hi")
