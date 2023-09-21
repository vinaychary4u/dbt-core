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

TEST__FAIL_WITH_VIEW_STRATEGY = """
{{ config(strategy="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__PASS_WITH_VIEW_STRATEGY = """
{{ config(strategy="view") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'grape'
"""


TEST__FAIL_WITH_TABLE_STRATEGY = """
{{ config(strategy="table") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'green'
"""


TEST__PASS_WITH_TABLE_STRATEGY = """
{{ config(strategy="table") }}
select *
from {{ ref('chipmunks') }}
where shirt = 'purple'
"""
