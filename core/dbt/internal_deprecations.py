import functools

from dbt.events.functions import warn_or_error
from dbt.events.types import FunctionDeprecated


def deprecated(reason="", suggested_action="", version=""):
    def inner(func):
        @functools.wraps(func)
        def wrapped(*args, **kwargs):
            function_name = func.__name__

            warn_or_error(
                FunctionDeprecated(
                    function_name=function_name,
                    reason=reason,
                    suggested_action=suggested_action,
                    version=version,
                )
            )  # TODO: pass in event?
            return func(*args, **kwargs)

        return wrapped

    return inner
