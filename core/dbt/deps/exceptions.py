import abc
import builtins
from typing import ClassVar, NoReturn, Dict

import dbt.events
from dbt.events.helpers import env_secrets, scrub_secrets


# TODO: using this special case here to not break the rest of exceptions but this would normally
# live centrally
class Exception(builtins.Exception):
    CODE = -32000
    MESSAGE = "Server Error"
    _event: ClassVar[str] = "GeneralException"
    _category: str = "general exception"

    def __init__(self):
        super().__init__()
        self.log()

    def data(self) -> Dict[str, str]:
        # do not override
        constant_exception_data = {
            "type": self.__class__.__name__,  # is this used outside logbook logs?
            "message": str(self),
            "event": self._event,  # does not always match type
            "category": self._category,
        }
        # TODO: can't guarantee this is always serializable...
        return {**constant_exception_data, **vars(self)}

    @property
    def event(self) -> abc.ABCMeta:
        if self._event is not None:
            module_path = dbt.events.types
            class_name = self._event

            try:
                return getattr(module_path, class_name)
            except AttributeError:
                msg = f"Event Class `{class_name}` is not defined in `{module_path}`"
                raise NameError(msg)
        raise NotImplementedError("event not implemented for {}".format(self))

    def log(self, *args, **kwargs) -> None:
        log_event = self.event(data=self.data(), **kwargs)
        dbt.events.functions.fire_event(log_event)


class RuntimeException(RuntimeError, Exception):
    CODE = 10001
    MESSAGE = "Runtime error"

    def __init__(self, msg, node=None):
        self.stack = []
        self.node = node
        self.message = scrub_secrets(msg, env_secrets())

    def add_node(self, node=None):
        if node is not None and node is not self.node:
            if self.node is not None:
                self.stack.append(self.node)
            self.node = node

    @property
    def type(self):
        return "Runtime"

    def node_to_string(self, node):
        if node is None:
            return "<Unknown>"
        if not hasattr(node, "name"):
            # we probably failed to parse a block, so we can't know the name
            return "{} ({})".format(node.resource_type, node.original_file_path)

        if hasattr(node, "contents"):
            # handle FileBlocks. They aren't really nodes but we want to render
            # out the path we know at least. This indicates an error during
            # block parsing.
            return "{}".format(node.path.original_file_path)
        return "{} {} ({})".format(node.resource_type, node.name, node.original_file_path)

    def process_stack(self):
        lines = []
        stack = self.stack + [self.node]
        first = True

        if len(stack) > 1:
            lines.append("")

            for item in stack:
                msg = "called by"

                if first:
                    msg = "in"
                    first = False

                lines.append("> {} {}".format(msg, self.node_to_string(item)))

        return lines

    def __str__(self, prefix="! "):
        node_string = ""

        if self.node is not None:
            node_string = " in {}".format(self.node_to_string(self.node))

        if hasattr(self.message, "split"):
            split_msg = self.message.split("\n")
        else:
            split_msg = str(self.message).split("\n")

        lines = ["{}{}".format(self.type + " Error", node_string)] + split_msg

        lines += self.process_stack()

        return lines[0] + "\n" + "\n".join(["  " + line for line in lines[1:]])

    def data(self):
        result = Exception.data(self)
        if self.node is None:
            return result

        result.update(
            {
                "raw_code": self.node.raw_code,
                # the node isn't always compiled, but if it is, include that!
                "compiled_code": getattr(self.node, "compiled_code", None),
            }
        )
        return result


# TODO: caused some circular imports. copied here. doesn't belong here.
class CommandError(RuntimeException):
    def __init__(self, cwd, cmd, message="Error running command"):
        cmd_scrubbed = list(scrub_secrets(cmd_txt, env_secrets()) for cmd_txt in cmd)
        super().__init__(message)
        self.cwd = cwd
        self.cmd = cmd_scrubbed
        self.args = (cwd, cmd_scrubbed, message)

    def __str__(self):
        if len(self.cmd) == 0:
            return "{}: No arguments given".format(self.message)
        return '{}: "{}"'.format(self.message, self.cmd[0])


# Start actual deps exceptions


class DependencyException(Exception):
    # this can happen due to raise_dependency_error and its callers
    CODE = 10006
    MESSAGE = "Dependency Error"
    _event: ClassVar[str] = "DependencyException"
    _category: str = "general deps"

    def __init__(self, message):
        super().__init__()
        self.message = scrub_secrets(message, env_secrets())


# TODO: explore where this should live
class ExecutableError(CommandError):
    def __init__(self, cwd, cmd, message):
        super().__init__(cwd, cmd, message)


class InternalException(DependencyException):
    _category: str = "internal"


# This was using SemverException previously...
class DependencyVersionException(DependencyException):
    _category: str = "version"

    def __init__(self, name):
        self.name = name
        msg = "Version error for package {}: {}".format(self.name, self)
        super().__init__(msg)


class MultipleDependencyVersionException(DependencyException):
    _category: str = "git"

    def __init__(self, git, requested):
        self.git = git
        self.requested = requested
        msg = "git dependencies should contain exactly one version. " "{} contains: {}".format(
            self.git, requested
        )
        super().__init__(msg)


class PackageNotFound(DependencyException):
    def __init__(self, package_name):
        self.package_name = package_name
        msg = f"Package {self.package_name} was not found in the package index"
        super().__init__(msg)


class PackageVersionNotFound(DependencyException):
    _category: str = "config"

    def __init__(self, package_name, version_range, available_versions, should_version_check):
        self.package_name = package_name
        self.version_range = str(version_range)
        self.available_versions = available_versions
        self.should_version_check = should_version_check
        msg = self.build_msg()
        super().__init__(msg)

    def build_msg(self):
        base_msg = (
            "Could not find a matching compatible version for package {}\n"
            "  Requested range: {}\n"
            "  Compatible versions: {}\n"
        )
        addendum = (
            (
                "\n"
                "  Not shown: package versions incompatible with installed version of dbt-core\n"
                "  To include them, run 'dbt --no-version-check deps'"
            )
            if self.should_version_check
            else ""
        )
        return (
            base_msg.format(self.package_name, self.version_range, self.available_versions)
            + addendum
        )


# should these all become their own exceptions?  They have to all share a category if not.
def raise_dependency_error(msg) -> NoReturn:
    raise DependencyException(scrub_secrets(msg, env_secrets()))
