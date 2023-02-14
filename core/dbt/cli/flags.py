# TODO  Move this to /core/dbt/flags.py when we're ready to break things
import os
import sys
from dataclasses import dataclass
from importlib import import_module
from multiprocessing import get_context
from pprint import pformat as pf
from typing import Set, List

from click import Context, get_current_context, BadOptionUsage, Command
from click.core import ParameterSource

from dbt.config.profile import read_user_config
from dbt.contracts.project import UserConfig
import dbt.cli.params as p
from dbt.helper_types import WarnErrorOptions
from dbt.cli.resolvers import default_project_dir, default_log_path


if os.name != "nt":
    # https://bugs.python.org/issue41567
    import multiprocessing.popen_spawn_posix  # type: ignore  # noqa: F401

# For backwards compatability, some params are defined across multiple levels,
# Top-level value should take precedence.
# e.g. dbt --target-path test2 run --target-path test2
EXPECTED_DUPLICATE_PARAMS = [
    "full_refresh",
    "target_path",
    "version_check",
    "fail_fast",
    "indirect_selection",
    "store_failures",
]


def convert_config(config_name, config_value):
    # This function should take care of converting the values from config and original
    # set_from_args to the correct type
    ret = config_value
    if config_name.lower() == "warn_error_options":
        ret = WarnErrorOptions(
            include=config_value.get("include", []), exclude=config_value.get("exclude", [])
        )
    return ret


@dataclass(frozen=True)
class Flags:
    def __init__(self, ctx: Context = None, user_config: UserConfig = None) -> None:

        if ctx is None:
            try:
                ctx = get_current_context(silent=True)
            except RuntimeError:
                ctx = None

        def assign_params(ctx, params_assigned_from_default, params_assigned_from_user):
            """Recursively adds all click params to flag object"""
            for param_name, param_value in ctx.params.items():
                # N.B. You have to use the base MRO method (object.__setattr__) to set attributes
                # when using frozen dataclasses.
                # https://docs.python.org/3/library/dataclasses.html#frozen-instances
                if param_name in EXPECTED_DUPLICATE_PARAMS:
                    # Expected duplicate param from multi-level click command (ex: dbt --full_refresh run --full_refresh)
                    # Overwrite user-configured param with value from parent context
                    if ctx.get_parameter_source(param_name) != ParameterSource.DEFAULT:
                        object.__setattr__(self, param_name.upper(), param_value)
                        params_assigned_from_user.add(param_name)
                else:
                    object.__setattr__(self, param_name.upper(), param_value)
                    params_assigned_from_user.add(param_name)
                    if ctx.get_parameter_source(param_name) == ParameterSource.DEFAULT:
                        params_assigned_from_default.add(param_name)
                        params_assigned_from_user.remove(param_name)

            if ctx.parent:
                assign_params(ctx.parent, params_assigned_from_default, params_assigned_from_user)

        params_assigned_from_default = set()  # type: Set[str]
        params_assigned_from_user = set()  # type: Set[str]
        which = None

        if ctx:
            # Assign params from ctx
            assign_params(ctx, params_assigned_from_default, params_assigned_from_user)

            # Get the invoked command flags
            invoked_subcommand_name = (
                ctx.invoked_subcommand if hasattr(ctx, "invoked_subcommand") else None
            )
            if invoked_subcommand_name is not None:
                invoked_subcommand = getattr(
                    import_module("dbt.cli.main"), invoked_subcommand_name
                )
                invoked_subcommand.allow_extra_args = True
                invoked_subcommand.ignore_unknown_options = True
                invoked_subcommand_ctx = invoked_subcommand.make_context(None, sys.argv)
                assign_params(
                    invoked_subcommand_ctx, params_assigned_from_default, params_assigned_from_user
                )

            which = invoked_subcommand_name or ctx.info_name

        if not user_config:
            profiles_dir = getattr(self, "PROFILES_DIR", None)
            user_config = read_user_config(profiles_dir) if profiles_dir else None

        # Overwrite default assignments with user config if available
        if user_config:
            for param_assigned_from_default in params_assigned_from_default:
                user_config_param_value = getattr(user_config, param_assigned_from_default, None)
                if user_config_param_value is not None:
                    object.__setattr__(
                        self,
                        param_assigned_from_default.upper(),
                        convert_config(param_assigned_from_default, user_config_param_value),
                    )
                    params_assigned_from_user.add(param_assigned_from_default)

        # Hard coded flags
        object.__setattr__(self, "WHICH", which)
        object.__setattr__(self, "MP_CONTEXT", get_context("spawn"))

        # Default LOG_PATH from PROJECT_DIR, if available.
        if getattr(self, "LOG_PATH", None) is None:
            project_dir = getattr(self, "PROJECT_DIR", default_project_dir())
            version_check = getattr(self, "VERSION_CHECK", True)
            object.__setattr__(self, "LOG_PATH", default_log_path(project_dir, version_check))

        # Support console DO NOT TRACK initiave

        if os.getenv("DO_NOT_TRACK", "").lower() in ("1", "t", "true", "y", "yes"):
            object.__setattr__(self, "SEND_ANONYMOUS_USAGE_STATS", False)

        # Check mutual exclusivity once all flags are set
        self._assert_mutually_exclusive(
            params_assigned_from_user, ["WARN_ERROR", "WARN_ERROR_OPTIONS"]
        )

        # Support lower cased access for legacy code
        params = set(
            x for x in dir(self) if not callable(getattr(self, x)) and not x.startswith("__")
        )
        for param in params:
            object.__setattr__(self, param.lower(), getattr(self, param))

    def __str__(self) -> str:
        return str(pf(self.__dict__))

    def __getattribute__(self, name):
        try:
            return object.__getattribute__(self, name)
        except AttributeError:
            return self.get_default(name)

    def _assert_mutually_exclusive(
        self, params_assigned_from_user: Set[str], group: List[str]
    ) -> None:
        """
        Ensure no elements from group are simultaneously provided by a user, as inferred from params_assigned_from_user.
        Raises click.UsageError if any two elements from group are simultaneously provided by a user.
        """
        set_flag = None
        for flag in group:
            flag_set_by_user = flag.lower() in params_assigned_from_user
            if flag_set_by_user and set_flag:
                raise BadOptionUsage(
                    flag.lower(), f"{flag.lower()}: not allowed with argument {set_flag.lower()}"
                )
            elif flag_set_by_user:
                set_flag = flag

    @classmethod
    def get_default(cls, param_name: str):
        param_decorator_name = param_name.lower()

        try:
            param_decorator = getattr(p, param_decorator_name)
        except AttributeError:
            raise AttributeError(f"'{cls.__name__}' object has no attribute '{param_name}'")

        command = param_decorator(Command(None))
        param = command.params[0]
        default = param.default
        if callable(default):
            return default()
        else:
            if param.type:
                try:
                    return param.type.convert(default, param, None)
                except TypeError:
                    return default
            return default
