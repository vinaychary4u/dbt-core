from dataclasses import dataclass
import os
import threading
from datetime import datetime
import dbt.events.proto_types as pt

# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #
# These base types define the _required structure_ for the concrete event #
# types defined in types.py                                               #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #


class Cache:
    # Events with this class will only be logged when the `--log-cache-events` flag is passed
    pass


def get_global_metadata_vars() -> dict:
    from dbt.events.functions import get_metadata_vars

    return get_metadata_vars()


def get_invocation_id() -> str:
    from dbt.events.functions import get_invocation_id

    return get_invocation_id()


# exactly one pid per concrete event
def get_pid() -> int:
    return os.getpid()


# preformatted time stamp
def get_ts_rfc3339() -> str:
    ts = datetime.utcnow()
    ts_rfc3339 = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ts_rfc3339


# in theory threads can change so we don't cache them.
def get_thread_name() -> str:
    return threading.current_thread().name


@dataclass
class BaseEvent:
    """BaseEvent for proto message generated python events"""

    def level_tag(self) -> str:
        return "debug"

    def message(self):
        raise Exception("message() not implemented for event")


# Create with level, msg,
@dataclass
class Event(pt.Event):
    def __post_init__(self):
        super().__post_init__()
        self.invocation_id = get_invocation_id()
        self.extra = get_global_metadata_vars()
        self.ts = datetime.utcnow()
        self.pid = get_pid()
        self.thread = get_thread_name()
        self.name = type(self).__name__


# DynamicLevel requires that the level be supplied on the
# fire_event call using the "level" keyword parameter
@dataclass  # type: ignore[misc]
class DynamicLevel(BaseEvent):
    pass


@dataclass
class TestLevel(BaseEvent):
    __test__ = False

    def level_tag(self) -> str:
        return "test"


@dataclass  # type: ignore[misc]
class DebugLevel(BaseEvent):
    def level_tag(self) -> str:
        return "debug"


@dataclass  # type: ignore[misc]
class InfoLevel(BaseEvent):
    def level_tag(self) -> str:
        return "info"


@dataclass  # type: ignore[misc]
class WarnLevel(BaseEvent):
    def level_tag(self) -> str:
        return "warn"


@dataclass  # type: ignore[misc]
class ErrorLevel(BaseEvent):
    def level_tag(self) -> str:
        return "error"


# Included to ensure classes with str-type message members are initialized correctly.
@dataclass  # type: ignore[misc]
class AdapterEventStringFunctor:
    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.base_msg, str):
            self.base_msg = str(self.base_msg)


@dataclass  # type: ignore[misc]
class EventStringFunctor:
    def __post_init__(self):
        super().__post_init__()
        if not isinstance(self.msg, str):
            self.msg = str(self.msg)


# prevents an event from going to the file
# This should rarely be used in core code. It is currently
# only used in integration tests and for the 'clean' command.
class NoFile:
    pass


# prevents an event from going to stdout
class NoStdOut:
    pass
