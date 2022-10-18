import betterproto
from colorama import Style
from dbt.events.base_types import NoStdOut, BaseEvent, NoFile, Cache
from dbt.events.types import EventBufferFull, MainReportVersion, EmptyLine
import dbt.flags as flags
from dbt.constants import SECRET_ENV_PREFIX, METADATA_ENV_PREFIX

from dbt.logger import make_log_dir_if_missing, GLOBAL_LOGGER
from datetime import datetime
import json
import io
from io import StringIO, TextIOWrapper
import logbook
import logging
from logging import Logger
import sys
from logging.handlers import RotatingFileHandler
import os
import uuid
import threading
from typing import List, Optional, Union, Callable, Dict, Any
from collections import deque

LOG_VERSION = 3
EVENT_HISTORY = None

# create the global file logger with no configuration
FILE_LOG = logging.getLogger("default_file")
null_handler = logging.NullHandler()
FILE_LOG.addHandler(null_handler)

# set up logger to go to stdout with defaults
# setup_event_logger will be called once args have been parsed
STDOUT_LOG = logging.getLogger("default_stdout")
STDOUT_LOG.setLevel(logging.INFO)
stdout_handler = logging.StreamHandler(sys.stdout)
stdout_handler.setLevel(logging.INFO)
STDOUT_LOG.addHandler(stdout_handler)

format_color = True
format_json = False
using_protobuf = False
invocation_id: Optional[str] = None
metadata_vars: Optional[Dict[str, str]] = None


def setup_event_logger(log_path, level_override=None):
    global format_json, format_color, STDOUT_LOG, FILE_LOG
    make_log_dir_if_missing(log_path)

    format_json = flags.LOG_FORMAT == "json"
    # USE_COLORS can be None if the app just started and the cli flags
    # havent been applied yet
    format_color = True if flags.USE_COLORS else False
    # TODO this default should live somewhere better
    level = level_override or (logging.DEBUG if flags.DEBUG else logging.INFO)

    # overwrite the STDOUT_LOG logger with the configured one
    STDOUT_LOG = logging.getLogger("configured_std_out")
    STDOUT_LOG.setLevel(level)

    stdout_passthrough_formatter = logging.Formatter(fmt="%(message)s")

    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(stdout_passthrough_formatter)
    stdout_handler.setLevel(level)
    # clear existing stdout TextIOWrapper stream handlers
    STDOUT_LOG.handlers = [
        h
        for h in STDOUT_LOG.handlers
        if not (hasattr(h, "stream") and isinstance(h.stream, TextIOWrapper))  # type: ignore
    ]
    STDOUT_LOG.addHandler(stdout_handler)

    global using_protobuf
    using_protobuf = isinstance(flags.LOG_FORMAT, str) and flags.LOG_FORMAT.lower() == "protobuf"
    log_filename = os.path.join(log_path, "dbt.binlog" if using_protobuf else "dbt.log")

    file_logger = logging.getLogger("configured_file")
    file_logger.setLevel(logging.DEBUG)  # always debug regardless of user input
    if using_protobuf:
        configure_protobuf_binary_logger(file_logger, log_filename)
    else:
        configure_text_file_logger(file_logger, log_filename)

    # overwrite the FILE_LOG logger with the configured one
    FILE_LOG = file_logger


def configure_text_file_logger(logger: logging.Logger, log_filename: str) -> None:
    file_passthrough_formatter = logging.Formatter(fmt="%(message)s")
    file_handler = RotatingFileHandler(
        filename=log_filename, encoding="utf8", maxBytes=10 * 1024 * 1024, backupCount=5  # 10 mb
    )
    file_handler.setFormatter(file_passthrough_formatter)
    file_handler.setLevel(logging.DEBUG)  # always debug regardless of user input
    logger.handlers.clear()
    logger.addHandler(file_handler)


class ProtobufFormatter(logging.Formatter):
    def format(self, record):
        return create_protobuf_log_block(record.msg)


def configure_protobuf_binary_logger(logger: logging.Logger, log_filename: str) -> None:
    handler = RotatingFileHandler(
        filename=log_filename,
        mode="ab",  # append in binary mode
        backupCount=5,
    )
    handler.terminator = b""  # type: ignore
    # Setting maxByts *after* the above constructor works around a python
    # logging inconsistency for binary logs.
    handler.maxBytes = 10 * 1024 * 1024  # type: ignore
    handler.setFormatter(ProtobufFormatter())
    logger.addHandler(handler)


# used for integration tests
def capture_stdout_logs() -> StringIO:
    global STDOUT_LOG
    capture_buf = io.StringIO()
    stdout_capture_handler = logging.StreamHandler(capture_buf)
    stdout_handler.setLevel(logging.DEBUG)
    STDOUT_LOG.addHandler(stdout_capture_handler)
    return capture_buf


# used for integration tests
def stop_capture_stdout_logs() -> None:
    global STDOUT_LOG
    STDOUT_LOG.handlers = [
        h
        for h in STDOUT_LOG.handlers
        if not (hasattr(h, "stream") and isinstance(h.stream, StringIO))  # type: ignore
    ]


def env_secrets() -> List[str]:
    return [v for k, v in os.environ.items() if k.startswith(SECRET_ENV_PREFIX) and v.strip()]


def scrub_secrets(msg: str, secrets: List[str]) -> str:
    scrubbed = msg

    for secret in secrets:
        scrubbed = scrubbed.replace(secret, "*****")

    return scrubbed


# returns a dictionary representation of the event fields.
# the message may contain secrets which must be scrubbed at the usage site.
def event_to_json(
    event: BaseEvent,
) -> str:
    event_dict = event_to_dict(event)
    raw_log_line = json.dumps(event_dict, sort_keys=True)
    return raw_log_line


def event_to_dict(event: BaseEvent) -> dict:
    event_dict = dict()
    try:
        # We could use to_json here, but it wouldn't sort the keys.
        # The 'to_json' method just does json.dumps on the dict anyway.
        event_dict = event.to_dict(casing=betterproto.Casing.SNAKE, include_default_values=True)  # type: ignore
    except AttributeError as exc:
        event_type = type(event).__name__
        raise Exception(f"type {event_type} is not serializable. {str(exc)}")
    return event_dict


# translates an Event to a completely formatted text-based log line
# type hinting everything as strings so we don't get any unintentional string conversions via str()
def reset_color() -> str:
    global format_color
    return "" if not format_color else Style.RESET_ALL


def create_info_text_log_line(e: BaseEvent) -> str:
    color_tag: str = reset_color()
    ts: str = get_ts().strftime("%H:%M:%S")  # TODO: get this from the event.ts?
    scrubbed_msg: str = scrub_secrets(e.message(), env_secrets())
    log_line: str = f"{color_tag}{ts}  {scrubbed_msg}"
    return log_line


def create_debug_text_log_line(e: BaseEvent) -> str:
    log_line: str = ""
    # Create a separator if this is the beginning of an invocation
    if type(e) == MainReportVersion:
        separator = 30 * "="
        log_line = f"\n\n{separator} {get_ts()} | {get_invocation_id()} {separator}\n"
    color_tag: str = reset_color()
    ts: str = get_ts().strftime("%H:%M:%S.%f")
    scrubbed_msg: str = scrub_secrets(e.message(), env_secrets())
    # Make the levels all 5 characters so they line up
    level: str = f"{e.level_tag():<5}"
    thread = ""
    if threading.current_thread().name:
        thread_name = threading.current_thread().name
        thread_name = thread_name[:10]
        thread_name = thread_name.ljust(10, " ")
        thread = f" [{thread_name}]:"
    log_line = log_line + f"{color_tag}{ts} [{level}]{thread} {scrubbed_msg}"
    return log_line


# translates an Event to a completely formatted json log line
def create_json_log_line(e: BaseEvent) -> Optional[str]:
    if type(e) == EmptyLine:
        return None  # will not be sent to logger
    raw_log_line = event_to_json(e)
    return scrub_secrets(raw_log_line, env_secrets())


# calls create_stdout_text_log_line() or create_json_log_line() according to logger config
def create_log_line(e: BaseEvent, file_output=False) -> Optional[str]:
    global format_json
    if format_json:
        return create_json_log_line(e)  # json output, both console and file
    elif file_output is True or flags.DEBUG:
        return create_debug_text_log_line(e)  # default file output
    else:
        return create_info_text_log_line(e)  # console output


def create_protobuf_log_block(e: betterproto.Message) -> bytes:
    event_name = type(e).__name__
    event_name_bytes = bytes(event_name, "utf-8")
    event_proto_bytes = bytes(e)

    # The encoded byte block for an entry in the binary event log consists of
    # the following four byte sequences, concatenated:
    #
    # 1. A 16-bit integer, unsigned, little-endian
    # 2. The event type name, a UTF-8 string with byte-length given by (1)
    # 3. A 32-bit integer, unsigned, little-endian
    # 4. A protobuf message of type given by (2) and byte-length given by (3)
    block_bytes: bytes = (
        len(event_name_bytes).to_bytes(2, "little", signed=False)
        + event_name_bytes
        + len(event_proto_bytes).to_bytes(4, "little", signed=False)
        + event_proto_bytes
    )
    # TODO: Create and apply a scrub_secrets() equivalent for protobuf binary log messages
    return block_bytes


# allows for reuse of this obnoxious if else tree.
# do not use for exceptions, it doesn't pass along exc_info, stack_info, or extra
def send_to_logger(l: Union[Logger, logbook.Logger], level_tag: str, log_line: Any):
    if not log_line:
        return
    if level_tag == "test":
        # TODO after implmenting #3977 send to new test level
        l.debug(log_line)
    elif level_tag == "debug":
        l.debug(log_line)
    elif level_tag == "info":
        l.info(log_line)
    elif level_tag == "warn":
        l.warning(log_line)
    elif level_tag == "error":
        l.error(log_line)
    else:
        raise AssertionError(
            f"While attempting to log {log_line}, encountered the unhandled level: {level_tag}"
        )


# an alternative to fire_event which only creates and logs the event value
# if the condition is met. Does nothing otherwise.
def fire_event_if(conditional: bool, lazy_e: Callable[[], BaseEvent]) -> None:
    if conditional:
        fire_event(lazy_e())


# top-level method for accessing the new eventing system
# this is where all the side effects happen branched by event type
# (i.e. - appending to event history, printing to stdout, logging
# to files, etc.)
def fire_event(e: BaseEvent) -> None:
    # skip logs when `--log-cache-events` is not passed
    if isinstance(e, Cache) and not flags.LOG_CACHE_EVENTS:
        return

    add_to_event_history(e)

    # backwards compatibility for plugins that require old logger (dbt-rpc)
    if flags.ENABLE_LEGACY_LOGGER:
        # using Event::message because the legacy logger didn't differentiate messages by
        # destination
        log_line = create_log_line(e)
        if log_line:
            send_to_logger(GLOBAL_LOGGER, e.level_tag(), log_line)
        return  # exit the function to avoid using the current logger as well

    # always logs debug level regardless of user input
    if not isinstance(e, NoFile):
        if using_protobuf:
            send_to_logger(FILE_LOG, level_tag=e.level_tag(), log_line=e)
        else:
            log_line = create_log_line(e, file_output=True)
            # doesn't send exceptions to exception logger
            if log_line:
                send_to_logger(FILE_LOG, level_tag=e.level_tag(), log_line=log_line)

    if not isinstance(e, NoStdOut):
        # explicitly checking the debug flag here so that potentially expensive-to-construct
        # log messages are not constructed if debug messages are never shown.
        if e.level_tag() == "debug" and not flags.DEBUG:
            return  # eat the message in case it was one of the expensive ones
        if e.level_tag() != "error" and flags.QUIET:
            return  # eat all non-exception messages in quiet mode

        log_line = create_log_line(e)
        if log_line:
            send_to_logger(STDOUT_LOG, level_tag=e.level_tag(), log_line=log_line)


def get_metadata_vars() -> Dict[str, str]:
    global metadata_vars
    if metadata_vars is None:
        metadata_vars = {
            k[len(METADATA_ENV_PREFIX) :]: v
            for k, v in os.environ.items()
            if k.startswith(METADATA_ENV_PREFIX)
        }
    return metadata_vars


def reset_metadata_vars() -> None:
    global metadata_vars
    metadata_vars = None


def get_invocation_id() -> str:
    global invocation_id
    if invocation_id is None:
        invocation_id = str(uuid.uuid4())
    return invocation_id


def set_invocation_id() -> None:
    # This is primarily for setting the invocation_id for separate
    # commands in the dbt servers. It shouldn't be necessary for the CLI.
    global invocation_id
    invocation_id = str(uuid.uuid4())


# exactly one time stamp per concrete event
def get_ts() -> datetime:
    ts = datetime.utcnow()
    return ts


# preformatted time stamp
def get_ts_rfc3339() -> str:
    ts = get_ts()
    ts_rfc3339 = ts.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    return ts_rfc3339


def add_to_event_history(event):
    if flags.EVENT_BUFFER_SIZE == 0:
        return
    global EVENT_HISTORY
    if EVENT_HISTORY is None:
        reset_event_history()
    EVENT_HISTORY.append(event)
    # We only set the EventBufferFull message for event buffers >= 10,000
    if flags.EVENT_BUFFER_SIZE >= 10000 and len(EVENT_HISTORY) == (flags.EVENT_BUFFER_SIZE - 1):
        fire_event(EventBufferFull())


def reset_event_history():
    global EVENT_HISTORY
    EVENT_HISTORY = deque(maxlen=flags.EVENT_BUFFER_SIZE)
