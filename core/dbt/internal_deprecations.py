from dbt.events.functions import fire_event  # warn_or_error
from dbt.events.types import FunctionDeprecated


def deprecated(reason="", version="", suggested_action=""):
    print("Inside decorator")

    def inner(func):
        print("Inside inner")

        # code functionality here
        function_name = func.__name__

        # warn_or_error(
        fire_event(
            FunctionDeprecated(
                function_name=function_name,
                reason=reason,
                suggested_action=suggested_action,
                version=version,
            )
        )  # TODO: pass in event?

        return func

    # returning inner function
    return inner
