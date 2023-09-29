SEED__CHIPMUNKS = """
name,shirt
alvin,red
simon,blue
theodore,green
""".strip()


MODEL__CHIPMUNKS = """
{{ config(materialized='table') }}
select *
from {{ ref('chipmunks_stage') }}
"""

TEST__FAIL_AS_VIEW = """
{{ config(store_failures_as="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__PASS_AS_VIEW = """
{{ config(store_failures_as="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'grape'
"""


TEST__FAIL_AS_TABLE = """
{{ config(store_failures_as="table") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__PASS_AS_TABLE = """
{{ config(store_failures_as="table") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'purple'
"""
