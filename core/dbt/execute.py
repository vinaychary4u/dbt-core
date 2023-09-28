import re
from typing import Tuple
from agate import Table
from io import StringIO
from contextvars import ContextVar
from collections import defaultdict
import json
from dbt.contracts.connection import AdapterResponse
from dbt.flags import get_flags
from dbt.version import __version__ as dbt_version

executed = defaultdict(list)
current_node = ContextVar("current_node", default="")  # type: ignore

prev_execution_record = {}  # type: ignore
unmatched = defaultdict(list)
diffs = defaultdict(list)


def add_execution(sql: str, response: AdapterResponse, table: Table) -> None:
    buf = StringIO()
    table.to_json(buf)  # type: ignore
    executed[current_node.get()].append(
        {"sql": sql, "response": response.to_dict(), "table": buf.getvalue()}
    )


def cleanup_sql(sql):
    # Remove single-line comments (--)
    sql = re.sub(r"--.*?\n", "", sql)
    # Remove multi-line comments (/* */)
    sql = re.sub(r"/\*.*?\*/", "", sql, flags=re.DOTALL)
    return sql.replace(" ", "").replace("\n", "")


def matches(sql1, sql2):
    return cleanup_sql(sql1) == cleanup_sql(sql2)


def get_result(unique_id: str, sql: str) -> Tuple[dict, str]:
    if unique_id not in prev_execution_record:
        diffs[unique_id].append(
            {
                "current": sql,
                "prev": "",
            }
        )
        return {}, "{}"
    prev = prev_execution_record[unique_id].pop(0)
    if matches(sql, prev["sql"]):
        return prev["response"], prev["table"]
    else:
        for i, existing_results in enumerate(prev_execution_record[unique_id]):
            if matches(sql, existing_results["sql"]):
                prev_execution_record[unique_id].pop(i)
                return existing_results["response"], existing_results["table"]

        unmatched[unique_id].append(prev)
        diffs[unique_id].append({"current": sql, "prev": prev["sql"]})
        return {}, "{}"


def get_execution_result(sql) -> Tuple[AdapterResponse, Table]:
    response_dict, agate_json = get_result(current_node.get(), sql)
    if "_message" not in response_dict:
        response_dict["_message"] = ""
    response = AdapterResponse.from_dict(response_dict)
    table = Table.from_object(json.loads(agate_json))
    return response, table


def load_execution_record() -> None:
    global prev_execution_record
    prev_execution_record = json.load(open(get_flags().EXECUTION_RECORD_PATH, "r"))


def write_execution_record() -> None:
    json.dump(
        executed, open(f"{get_flags().TARGET_PATH or './target'}/executed-{dbt_version}.json", "w")
    )


def write_diffs() -> None:
    json.dump(
        diffs, open(f"{get_flags().TARGET_PATH or './target'}/diffs-{dbt_version}.json", "w")
    )
