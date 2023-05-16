model_one = """
    select 1 as id
"""

model_two = """
    select * from {{ ref("model_one") }}
"""

model_three = """
    breaking line
    select * from {{ ref("model_two") }}
"""

model_four = """
    select * from {{ ref("model_three") }}
"""

seed_one = """
animal_name,num_legs
dog,4
"""

snapshot_one = """
{% snapshot snapshot_one %}
    {{
        config(
            strategy="timestamp",
            target_database=var('target_database', database),
            target_schema=schema,
            updated_at="updated_at",
            unique_key="id",
        )
    }}
    select * from {{ ref("model_one") }}
{% endsnapshot %}
"""

snapshot_two = """
{% snapshot snapshot_two %}
    {{
        config(
            strategy="timestamp",
            target_database=var('target_database', database),
            target_schema=schema,
            updated_at="updated_at",
            unique_key="id",
        )
    }}
    select * from {{ ref("model_one") }}
    union
    select * from {{ ref("model_three") }}
{% endsnapshot %}
"""

