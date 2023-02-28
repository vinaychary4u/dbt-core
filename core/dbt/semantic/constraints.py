from __future__ import annotations

import re
from typing import List, Dict, Any
from dataclasses import dataclass, field
from dbt.contracts.util import Mergeable

from mo_sql_parsing import parse as mo_parse

from dbt.dataclass_schema import dbtClassMixin
from dbt.exceptions import DbtSemanticValidationError
from dbt.semantic.sql_bind_parameters import SqlBindParameters

LITERAL_STR = "literal"
INTERVAL_LITERAL = "interval"


@dataclass
class WhereClauseConstraint(dbtClassMixin, Mergeable):
    """Contains a string that is a where clause"""

    where_clause: str = ""
    linkable_names: List[str] = field(default_factory=list)
    sql_params: SqlBindParameters = SqlBindParameters()

    # def __init__(  # noqa: D
    #     self,
    #     where_clause: str = "",
    #     linkable_names: Optional[List[str]] = None,
    #     sql_params: Optional[SqlBindParameters] = None,
    #     # sql params: user-originated sql params that need to be escaped in a dialect-specific way keys are the
    #     # name of the template value in the `where` string, values are the string to be escaped and
    #     # inserted into the where string (ie where = "%(1)s", sql_values = {"1": "cote d'ivoire"})
    # ) -> WhereClauseConstraint:
    #     where_clause = where_clause.strip("\n") if where_clause else ""
    #     linkable_names = linkable_names or []
    #     if sql_params is None:
    #         sql_params = SqlBindParameters()
    #     super().__init__(
    #         where_clause=where_clause,
    #         linkable_names=linkable_names,
    #         sql_params=sql_params,
    #     )

    @staticmethod
    def parse(s: str) -> WhereClauseConstraint:
        """Parse a string into a WhereClauseConstraint

        We are assuming here that if we needed to parse a string, we wouldn't have bind parameters.
        Because if we had bind-parameters, the string would have existing structure, and we wouldn't need to parse it.
        """
        s = strip_where_clause(s)

        where_clause_str = f"WHERE {s}"
        # to piggyback on moz sql parser we need a SELECT statement
        # moz breaks the sql statement into clauses:
        # where_clause_str = "WHERE is_instant" yields -> {'select': {'value': '_'}, 'from': '_', 'where': 'is_instant'}
        # where_clause_str = "WHERE is_instant AND country = 'vanuatu' AND is_lux or ds < '2020-01-02'" yields ->
        # {'select': {'value': '_'}, 'from': '_', 'where_clause': {'or': [{'and': ['is_instant', {'eq': ['country', {'literal': 'vanuatu'}]}, 'is_lux']}, {'lt': ['ds', {'literal': '2020-01-02'}]}]}}
        parsed = mo_parse(f"select _ from _ {where_clause_str}")
        if "where" not in parsed:
            raise DbtSemanticValidationError(parsed)

        where_clause = parsed["where"]

        if isinstance(where_clause, dict):
            if not len(where_clause.keys()) == 1:
                raise DbtSemanticValidationError(
                    f"expected parsed constraint to contain exactly one key; got {where_clause}"
                )
            return WhereClauseConstraint(
                where_clause=s,
                linkable_names=constraint_dimension_names_from_dict(where_clause),
                sql_params=SqlBindParameters(),
            )
        elif isinstance(where_clause, str):
            return WhereClauseConstraint(
                where_clause=s,
                linkable_names=[where_clause.strip()],
                sql_params=SqlBindParameters(),
            )
        else:
            raise TypeError(
                f"where-clause is neither a dict nor a string. Unexpectedly it is a {type(where_clause)}"
            )

    def __repr__(self) -> str:  # noqa: D
        return (
            f"{self.__class__.__name__}"
            f"(where_clause={self.where_clause}, linkable_names={self.linkable_names})"
        )


def strip_where_clause(s: str) -> str:
    """Removes WHERE from the beginning of the string, if present (regardless of case)"""
    # '^' tells the regex to only check the beginning of the string
    return re.sub("^where ", "", s, flags=re.IGNORECASE)


def constraint_dimension_names_from_dict(where_clause: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: D
    dims = []
    for key, clause in where_clause.items():
        if key == LITERAL_STR or key == INTERVAL_LITERAL:
            continue
        dims += _get_dimensions_from_clause(clause)

    return dims


def constraint_values_from_dict(where_clause: Dict[str, Any]) -> List[str]:  # type: ignore[misc] # noqa: d
    values = []
    for key, clause in where_clause.items():
        if key == LITERAL_STR:
            values.append(clause)
        elif isinstance(clause, dict):
            values += constraint_values_from_dict(clause)
        elif isinstance(clause, list):
            for item in clause:
                if isinstance(item, dict):
                    values += constraint_values_from_dict(item)

    return values


def _constraint_dimensions_from_list(list_clause: List[Any]) -> List[str]:  # type: ignore[misc] # noqa: D
    dims = []
    for clause in list_clause:
        dims += _get_dimensions_from_clause(clause)

    return dims


def _get_dimensions_from_clause(clause: Any) -> List[str]:  # type: ignore[misc] # noqa: D
    if clause is not None:
        if isinstance(clause, dict):
            return constraint_dimension_names_from_dict(clause)
        elif isinstance(clause, list):
            return _constraint_dimensions_from_list(clause)
        elif isinstance(clause, str):
            return [clause.strip()]

    return []
