from typing import Dict


def row_count(project, schema: str, relation_name: str) -> int:
    # postgres only supports schema names of 63 characters
    # a schema with a longer name still gets created, but the name gets truncated
    schema_name = schema[:63]
    sql = f"select count(*) from {schema_name}.{relation_name}"
    return project.run_sql(sql, fetch="one")[0]


def insert_record(project, schema: str, table_name: str, record: Dict[str, str]):
    # postgres only supports schema names of 63 characters
    # a schema with a longer name still gets created, but the name gets truncated
    schema_name = schema[:63]
    field_names, field_values = [], []
    for field_name, field_value in record.items():
        field_names.append(field_name)
        field_values.append(f"'{field_value}'")
    field_name_clause = ", ".join(field_names)
    field_value_clause = ", ".join(field_values)

    sql = f"""
    insert into {schema_name}.{table_name} ({field_name_clause})
    values ({field_value_clause})
    """
    project.run_sql(sql)


def delete_record(project, schema: str, table_name: str, record: Dict[str, str]):
    schema_name = schema[:63]
    where_clause = " and ".join(
        [f"{field_name} = '{field_value}'" for field_name, field_value in record.items()]
    )
    sql = f"""
    delete from {schema_name}.{table_name}
    where {where_clause}
    """
    project.run_sql(sql)
