saved_queries_yml = """
version: 2

saved_queries:
  - name: test_saved_query
    description: A saved query for testing
    label: Test Saved Query
    metrics:
        - simple_metric
    group_bys:
        - "Dimension('user__ds')"
    where:
        - "{{ Dimension('user__ds', 'DAY') }} <= now()"
        - "{{ Dimension('user__ds', 'DAY') }} >= '2023-01-01'"
"""
