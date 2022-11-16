import sys
from dbt.events.types import (
    MainReportVersion,
    MainReportArgs,
    RollbackFailed,
    MainEncounteredError,
    PluginLoadError,
    LogStartLine,
    LogTestResult,
)
from dbt.events.base_types import Event
from dbt.events.functions import event_to_dict, LOG_VERSION, reset_metadata_vars, create_event
from dbt.events import proto_types as pt
from dbt.events import proto_event as pe
from dbt.version import installed


event_keys = {"code", "msg", "level", "invocation_id", "pid", "thread", "ts", "extra"}


def test_events():

    # A001 event
    detail_event = MainReportVersion(version=str(installed), log_version=LOG_VERSION)
    event = create_event(detail_event)
    event_dict = event_to_dict(event)
    print(f"--- event_dict: {event_dict}")
    event_json = event.to_json()
    serialized = bytes(event)
    assert "Running with dbt=" in str(serialized)
    assert set(event_dict["main_report_version"].keys()) == {"version", "log_version"}
    assert set(event_dict.keys()) == event_keys | {"main_report_version"}
    assert event_json
    assert event.code == "A001"

    # Extract event from serialized message
    event = pe.Event().parse(serialized)
    assert event.code == "A001"
    # look at the detail message

    # A002 event
    detail_event = MainReportArgs(args={"one": "1", "two": "2"})
    event = create_event(detail_event)
    event_dict = event_to_dict(event)
    event_json = event.to_json()

    assert set(event_dict["main_report_args"].keys()) == {"args"}
    assert set(event_dict.keys()) == event_keys | {"main_report_args"}
    assert event_json
    assert event.code == "A002"


def test_exception_events():
    detail_event = RollbackFailed(conn_name="test", exc_info="something failed")
    event = create_event(detail_event)
    event_dict = event_to_dict(event)
    event_json = event.to_json()
    assert set(event_dict["rollback_failed"].keys()) == {"conn_name", "exc_info"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "E009"

    event = PluginLoadError(exc_info="something failed")
    event_dict = event_to_dict(event)
    event_json = event.to_json()
    assert set(event_dict.keys()) == {"info", "exc_info"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "E036"
    # This event has no "msg"/"message"
    assert event.info.msg is None

    # Z002 event
    event = MainEncounteredError(exc="Rollback failed")
    event_dict = event_to_dict(event)
    event_json = event.to_json()

    assert set(event_dict.keys()) == {"info", "exc"}
    assert set(event_dict["info"].keys()) == info_keys
    assert event_json
    assert event.info.code == "Z002"


def test_node_info_events():
    node_info = {
        "node_path": "some_path",
        "node_name": "some_name",
        "unique_id": "some_id",
        "resource_type": "model",
        "materialized": "table",
        "node_status": "started",
        "node_started_at": "some_time",
        "node_finished_at": "another_time",
    }
    event = LogStartLine(
        description="some description",
        index=123,
        total=111,
        node_info=pt.NodeInfo(**node_info),
    )
    assert event
    assert event.node_info.node_path == "some_path"


def test_extra_dict_on_event(monkeypatch):

    monkeypatch.setenv("DBT_ENV_CUSTOM_ENV_env_key", "env_value")

    reset_metadata_vars()

    event = MainReportVersion(version=str(installed), log_version=LOG_VERSION)
    event_dict = event_to_dict(event)
    assert set(event_dict["info"].keys()) == info_keys
    assert event.info.extra == {"env_key": "env_value"}
    serialized = bytes(event)

    # Extract EventInfo from serialized message
    generic_event = pt.GenericMessage().parse(serialized)
    assert generic_event.info.code == "A001"
    # get the message class for the real message from the generic message
    message_class = getattr(sys.modules["dbt.events.proto_types"], generic_event.info.name)
    new_event = message_class().parse(serialized)
    assert new_event.info.extra == event.info.extra

    # clean up
    reset_metadata_vars()


def test_dynamic_level_events():
    event = LogTestResult(
        name="model_name",
        info=info(level=LogTestResult.status_to_level("pass")),
        status="pass",
        index=1,
        num_models=3,
        num_failures=0
    )
    assert event
    assert event.info.level == "info"
