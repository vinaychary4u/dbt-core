from enum import Enum
from typing import List, Union

from dbt.contracts.graph.manifest import Manifest
from dbt.contracts.results import (
    CatalogArtifact,
    RunExecutionResult,
    RunOperationResultsArtifact,
)
from dbt.exceptions import DbtInternalError


class Command(Enum):
    BUILD = "build"
    CLEAN = "clean"
    COMPILE = "compile"
    DOCS_GENERATE = "generate"
    DOCS_SERVE = "serve"
    DEBUG = "debug"
    DEPS = "deps"
    INIT = "init"
    LIST = "list"
    PARSE = "parse"
    RETRY = "retry"
    RUN = "run"
    RUN_OPERATION = "run-operation"
    SEED = "seed"
    SHOW = "show"
    SNAPSHOT = "snapshot"
    SOURCE_FRESHNESS = "freshness"
    TEST = "test"

    @classmethod
    def from_str(cls, s: str) -> "Command":
        try:
            return cls(s)
        except ValueError:
            raise DbtInternalError(f"No value '{s}' exists in Command enum")

    def to_list(self) -> List[str]:
        return {
            Command.DOCS_GENERATE: ["docs", "generate"],
            Command.DOCS_SERVE: ["docs", "serve"],
            Command.RUN_OPERATION: ["run-operation"],
            Command.SOURCE_FRESHNESS: ["source", "freshness"],
        }.get(self, [self.value])


CombinedResultType = Union[
    bool,  # debug
    CatalogArtifact,  # docs generate
    List[str],  # list/ls
    Manifest,  # parse
    None,  # clean, deps, init, source
    RunExecutionResult,  # build, compile, run, seed, snapshot, test
    RunOperationResultsArtifact,  # run-operation
]  # retry may return any of these
