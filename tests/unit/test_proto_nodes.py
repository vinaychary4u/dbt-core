from dbt.contracts.graph.parsed import ParsedModelNode
from dbt.node_types import NodeType
from dbt.contracts.files import FileHash


def test_nodes():
    # Create a dummy model node
    model_node = ParsedModelNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        language="sql",
        package_name="test",
        root_path="my/path",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.Model,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
    )
    assert model_node
    # Get a matching proto message
    proto_model_msg = model_node.to_msg()
    assert proto_model_msg
