from typing import Dict

from dbt.tests.adapter.persist_test_results.basic import PersistTestResults

from tests.functional.persist_test_results.utils import (
    delete_record,
    insert_record,
    row_count,
)


class TestPersistTestResults(PersistTestResults):
    def row_count(self, project, relation_name) -> int:
        return row_count(project, self.audit_schema, relation_name)

    def insert_record(self, project, record: Dict[str, str]):
        insert_record(project, project.test_schema, self.model_table, record)

    def delete_record(self, project, record: Dict[str, str]):
        delete_record(project, project.test_schema, self.model_table, record)
