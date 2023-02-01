from dbt.contracts.graph.nodes import (
    ModelNode,
    AnalysisNode,
    RPCNode,
    SqlNode,
    HookNode,
    SeedNode,
    SingularTestNode,
    GenericTestNode,
    TestMetadata,
    SnapshotNode,
    Macro,
    Documentation,
    SourceDefinition,
    Exposure,
    Metric,
)
from dbt.contracts.graph.model_config import (
    NodeConfig,
    SeedConfig,
    TestConfig,
    SnapshotConfig,
    SourceConfig,
    ExposureConfig,
    MetricConfig,
)
from dbt.contracts.graph.unparsed import ExposureOwner, MetricFilter
from dbt.node_types import NodeType, ModelLanguage
from dbt.contracts.files import FileHash


def test_nodes():
    # Create NodeConfig for use in the next 5 nodes
    node_config = NodeConfig(
        enabled=True,
        alias=None,
        schema="my_schema",
        database="my_database",
        tags=["one", "two", "three"],
        meta={"one": 1, "two": 2},
        materialized="table",
        incremental_strategy=None,
        full_refresh=None,
        on_schema_change="ignore",
        packages=["one", "two", "three"],
    )
    # Create a dummy ModelNode
    model_node = ModelNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        language="sql",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.Model,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=node_config,
    )
    assert model_node
    # Get a matching proto message
    proto_model_msg = model_node.to_msg()
    assert proto_model_msg

    # Create a dummy AnalysisNode
    analysis_node = AnalysisNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        language="sql",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.Analysis,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=node_config,
    )
    assert analysis_node
    # Get a matching proto message
    proto_analysis_msg = analysis_node.to_msg()
    assert proto_analysis_msg

    # Create a dummy RPCNode
    rpc_node = RPCNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        language="sql",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.RPCCall,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=node_config,
    )
    assert rpc_node
    # Get a matching proto message
    proto_rpc_msg = rpc_node.to_msg()
    assert proto_rpc_msg

    # Create a dummy SqlNode
    sql_node = SqlNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        language="sql",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.SqlOperation,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=node_config,
    )
    assert sql_node
    # Get a matching proto message
    proto_sql_msg = sql_node.to_msg()
    assert proto_sql_msg

    # Create a dummy HookNode
    hook_node = HookNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="hook.test.my_hook",
        raw_code="select 1 from fun",
        language="sql",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_hook",
        resource_type=NodeType.Operation,
        alias="my_hook",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=node_config,
        index=1,
    )
    assert hook_node
    # Get a matching proto message
    proto_hook_msg = hook_node.to_msg()
    assert proto_hook_msg
    assert proto_hook_msg.index

    # Create a dummy SeedNode
    seed_config = SeedConfig(
        enabled=True,
        alias=None,
        schema="my_schema",
        database="my_database",
        tags=["one", "two", "three"],
        meta={"one": 1, "two": 2},
    )
    seed_node = SeedNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.seed.my_node",
        raw_code="",
        package_name="test",
        path="seed.csv",
        original_file_path="seeds/seed.csv",
        name="seed",
        resource_type=NodeType.Seed,
        alias="seed",
        checksum=FileHash.from_contents("test"),
        root_path="some_path",
        config=seed_config,
    )
    assert seed_node
    # Get a matching proto message
    proto_seed_msg = seed_node.to_msg()
    assert proto_seed_msg
    assert proto_seed_msg.root_path

    # config for SingularTestNode and GenericTestNode
    test_config = TestConfig(
        enabled=True,
        alias=None,
        schema="my_schema",
        database="my_database",
        tags=["one", "two", "three"],
        meta={"one": 1, "two": 2},
    )

    # Create a dummy SingularTestNode
    singular_test_node = SingularTestNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.Test,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=test_config,
    )
    assert singular_test_node
    # Get a matching proto message
    proto_singular_test_msg = singular_test_node.to_msg()
    assert proto_singular_test_msg

    # Create a dummy GenericTestNode
    test_metadata = TestMetadata(
        name="my_test",
        kwargs={"one": 1, "two": "another"},
    )
    generic_test_node = GenericTestNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_node",
        raw_code="select 1 from fun",
        package_name="test",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_node",
        resource_type=NodeType.Test,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=test_config,
        test_metadata=test_metadata,
        column_name="some_column",
    )
    assert generic_test_node
    # Get a matching proto message
    proto_generic_test_msg = generic_test_node.to_msg()
    assert proto_generic_test_msg
    assert proto_generic_test_msg.column_name

    # Create SnapshotConfig and SnapshotNode
    snapshot_config = SnapshotConfig(
        enabled=True,
        alias=None,
        schema="my_schema",
        database="my_database",
        tags=["one", "two", "three"],
        meta={"one": 1, "two": 2},
        materialized="table",
        incremental_strategy=None,
        full_refresh=None,
        on_schema_change="ignore",
        packages=["one", "two", "three"],
        strategy="check",
        target_schema="some_schema",
        target_database="some_database",
        check_cols="id",
    )
    snapshot_node = SnapshotNode(
        database="testdb",
        schema="testschema",
        fqn=["my", "test"],
        unique_id="test.model.my_test",
        raw_code="select 1 from fun",
        language="sql",
        package_name="my_project",
        path="my_node.sql",
        original_file_path="models/my_node.sql",
        name="my_test",
        resource_type=NodeType.Snapshot,
        alias="my_node",
        checksum=FileHash.from_contents("select 1 from fun"),
        config=snapshot_config,
    )
    assert snapshot_node
    # Get a matching proto message
    proto_snapshot_msg = snapshot_node.to_msg()
    assert proto_snapshot_msg
    assert proto_snapshot_msg.config.target_schema

    # Create a dummy Macro
    macro = Macro(
        name="my_macro",
        resource_type=NodeType.Macro,
        package_name="my_project",
        path="my_macro.sql",
        original_file_path="macros/my_macro.sql",
        unique_id="macro.my_project.my_macro",
        macro_sql="{{ }}",
        description="my macro",
        supported_languages=[ModelLanguage.sql],
    )
    proto_macro_msg = macro.to_msg()
    assert proto_macro_msg
    assert proto_macro_msg.supported_languages == ["sql"]

    # Create a dummy Documentation
    doc = Documentation(
        name="my_doc",
        resource_type=NodeType.Macro,
        package_name="my_project",
        path="readme.md",
        original_file_path="models/readme.md",
        unique_id="doc.my_project.my_doc",
        block_contents="this is my special doc",
    )
    proto_doc_msg = doc.to_msg()
    assert proto_doc_msg
    assert proto_doc_msg.block_contents

    # Dummy SourceDefinition
    source = SourceDefinition(
        name="my_source",
        resource_type=NodeType.Source,
        package_name="my_project",
        path="source.yml",
        original_file_path="source.yml",
        unique_id="source.my_project.my_source",
        fqn=["sources", "my_source"],
        database="my_database",
        schema="my_schema",
        source_name="my_source",
        source_description="my source",
        loader="loader",
        identifier="my_source",
        config=SourceConfig(enabled=True),
    )
    proto_source_msg = source.to_msg()
    assert proto_source_msg
    assert proto_source_msg.source_name

    # Dummy Exposure
    exposure = Exposure(
        name="my_exposure",
        resource_type=NodeType.Exposure,
        package_name="my_project",
        path="exposure.yml",
        original_file_path="exposure.yml",
        unique_id="exposure.my_project.my_exposure",
        fqn=["my", "exposure"],
        config=ExposureConfig(enabled=True),
        type="dashboard",
        owner=ExposureOwner(email="someone@somewhere"),
        description="my exposure",
    )
    proto_exposure_msg = exposure.to_msg()
    assert proto_exposure_msg
    assert proto_exposure_msg.type

    # Dummy Metric
    metric = Metric(
        name="my_metric",
        resource_type=NodeType.Metric,
        package_name="my_project",
        path="metrics.yml",
        original_file_path="metrics.yml",
        unique_id="metric.my_project.my_metric",
        fqn=["my", "metric"],
        config=MetricConfig(enabled=True),
        description="my metric",
        label="my label",
        calculation_method="*",
        expression="select 1 as fun",
        filters=[MetricFilter(field="sum", operator="sum", value="sum")],
        time_grains=["day", "minute"],
        dimensions=["day", "minute"],
    )
    proto_metric_msg = metric.to_msg()
    assert proto_metric_msg
    assert proto_metric_msg.label
