"""
This will be in the `dbt-prql` package, but including here during inital code review, so
we can test it without coordinating dependencies.
"""

from __future__ import annotations

import logging
import re

from typing import Dict, Any

from dbt.parser.languages.provider import LanguageProvider, dbt_function_calls, references_type
from dbt.contracts.graph.nodes import ManifestNode


# import prql_python
# This mocks the prqlc output for two cases which we currently use in tests, so we can
# test this without configuring dependencies. (Obv fix as we expand the tests, way
# before we merge.)
class prql_python:  # type: ignore
    @staticmethod
    def to_sql(prql) -> str:

        query_1 = "from employees"

        query_1_compiled = """
SELECT
  employees.*
FROM
  employees
        """.strip()

        query_2 = """
from (dbt source.whatever.some_tbl)
join (dbt ref.test.foo) [id]
filter salary > 100
        """.strip()

        # hard coded for Jerco's Postgres database
        query_2_resolved = """
from ("jerco"."salesforce"."in_process")
join ("jerco"."dbt_jcohen"."foo") [id]
filter salary > 100
        """.strip()

        query_2_compiled = """
SELECT
"jerco"."whatever"."some_tbl".*,
"jerco"."dbt_jcohen"."foo".*,
id
FROM
"jerco"."salesforce"."in_process"
JOIN "jerco"."dbt_jcohen"."foo" USING(id)
WHERE
salary > 100
        """.strip()

        lookup = dict(
            {
                query_1: query_1_compiled,
                query_2: query_2_compiled,
                query_2_resolved: query_2_compiled,
            }
        )
        return lookup[prql]


logger = logging.getLogger(__name__)

word_regex = r"[\w\.\-_]+"
# TODO support single-argument form of 'ref'
references_regex = rf"\bdbt `?(\w+)\.({word_regex})\.({word_regex})`?"


def hack_compile(prql: str, references: references_type, dialect: str) -> str:
    """
    >>> print(compile(
    ...     "from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar) [id]",
    ...     references=dict(
    ...         sources={('salesforce', 'in_process'): 'salesforce_schema.in_process_tbl'},
    ...         refs={('foo', 'bar'): 'foo_schema.bar_tbl'}
    ...     )
    ... ))
    SELECT
      "{{ source('salesforce', 'in_process') }}".*,
      "{{ ref('foo', 'bar') }}".*,
      id
    FROM
      {{ source('salesforce', 'in_process') }}
      JOIN {{ ref('foo', 'bar') }} USING(id)
    """

    subs = []
    for k, v in references["sources"].items():
        key = ".".join(k)
        lookup = f"dbt source.{key}"
        subs.append((lookup, str(v)))

    for k, v in references["refs"].items():
        key = ".".join(k)
        lookup = f"dbt ref.{key}"
        subs.append((lookup, str(v)))

    for lookup, resolved in subs:
        prql = prql.replace(lookup, resolved)

    sql = prql_python.to_sql(prql)
    return sql


def hack_list_references(prql):
    """
    List all references (e.g. sources / refs) in a given block.

    We need to decide:

    — What should prqlc return given `dbt source.foo.bar`, so dbt-prql can find the
      references?
        — Should it just fill in something that looks like jinja for expediancy? (We
          don't support jinja though)

    >>> references = list_references("from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar)")
    >>> dict(references)
    {'source': [('salesforce', 'in_process')], 'ref': [('foo', 'bar')]}
    """
    out = []
    for t, package, model in _hack_references_of_prql_query(prql):
        out.append((t, [package, model], {}))
    return out


def _hack_references_of_prql_query(prql) -> list[tuple[str, str, str]]:
    """
    List the references in a prql query.

    This would be implemented by prqlc.

    >>> _hack_references_of_prql_query("from (dbt source.salesforce.in_process) | join (dbt ref.foo.bar)")
    [('source', 'salesforce', 'in_process'), ('ref', 'foo', 'bar')]
    """
    return re.findall(references_regex, prql)


class PrqlProvider(LanguageProvider):
    def __init__(self) -> None:
        # TODO: Uncomment when dbt-prql is released
        # if not dbt_prql:
        #     raise ImportError(
        #         "dbt_prql is required and not found; try running `pip install dbt_prql`"
        #     )
        pass

    @classmethod
    def name(self) -> str:
        return "prql"

    @classmethod
    def compiled_language(self) -> str:
        return "sql"

    @classmethod
    def extract_dbt_function_calls(self, node) -> dbt_function_calls:
        return hack_list_references(node.raw_code)

    @classmethod
    def needs_compile_time_connection(self) -> bool:
        return False

    @classmethod
    def get_compiled_code(self, node: ManifestNode, context: Dict[str, Any]) -> str:
        dialect = context["target"]["type"]
        resolved_references = self.get_resolved_references(node, context)
        return hack_compile(node.raw_code, references=resolved_references, dialect=dialect)
