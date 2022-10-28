import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import networkx as nx
import sqlglot
from dbt.exceptions import InternalException
from dbt.graph import ResourceTypeSelector
from dbt.node_types import NodeType
from dbt.task.runnable import GraphRunnableTask, ManifestTask
from dbt.task.test import TestSelector
from sqlglot import exp


@dataclass
class ColumnLineage:
    name: str
    parent_model: str
    source_columns: List["ColumnLineage"]  # "parent" columns


@dataclass
class NodeLineage:
    name: str
    columns: List[ColumnLineage]

    def col_by_name(self, name: str) -> Optional["ColumnLineage"]:
        for col in self.columns:
            if col.name == name:
                return col

        return None


@dataclass
class Env():
    models: Dict[str, NodeLineage]



def collapse_lineage(lineage: NodeLineage) -> NodeLineage:
    collapsed_lineage = NodeLineage(
        name=lineage.name,
        columns=[ll for ll in lineage.columns]
    )

    for col in lineage.columns:
        new_sources = collapse_column_sources(col)
        col.source_columns = new_sources

    return collapsed_lineage


def collapse_column_sources(column: ColumnLineage) -> List[ColumnLineage]:
    finals = [c for c in column.source_columns if not is_internal(c)]
    internals = [c for c in column.source_columns if is_internal(c)]

    for internal in internals:
        finals += collapse_column_sources(internal)

    return finals


def is_internal(column: ColumnLineage):
    return column.parent_model == "_from_clause"  # this would be generalizaed when we have more internal column types, for now we only have from clause internals


# Details for generating a NodeLineage from a parsed select.
def get_lineage_by_select(env: Env, select: exp.Select, output_name: str) -> NodeLineage:
    lineage = NodeLineage(
       name=output_name,
       columns=[]
    )

    from_exp = select.args["from"]
    join_exps = select.args["joins"] if "joins" in select.args else []
    from_lineage = get_lineage_by_from(env, from_exp, join_exps)

    for col_select in select.selects:
        if col_select.key == "alias" or col_select.key == "column":
            source_name: str = col_select.this.alias_or_name
            aliased_name: str = source_name if not col_select.alias else col_select.alias
            source_lineage = from_lineage.col_by_name(source_name)
            lineage.columns.append(ColumnLineage(name=aliased_name, parent_model=output_name, source_columns=[source_lineage]))
        elif col_select.key == "star":
            for col in from_lineage.columns:
                lineage.columns.append(ColumnLineage(name=col.name, parent_model=output_name, source_columns=[col]))

    return lineage


# Details for generating a NodeLineage from a parsed select.
def get_lineage_by_from(env: Env, from_exp: exp.From, join_exps: List[exp.Join]) -> NodeLineage:
    from_lineage = NodeLineage(name="_from_clause", columns=[])
    name_args = from_exp.expressions[0].args
    model_full_name = (name_args["catalog"].name + "." if name_args["catalog"] else "") + (name_args["db"].name + "." if name_args["db"] else "") + name_args["this"].name
    
    cols = get_columns_by_table_name(env, model_full_name)
    from_lineage.columns += cols

    for join_exp in join_exps:
        join_exp.args["this"].name
        cols = get_columns_by_table_name(env, model_full_name)
        from_lineage.columns += cols

    return from_lineage


def get_columns_by_table_name(env: Env, table_name: str) -> List[ColumnLineage]:
    cols = []
    for col in env.models[table_name].columns:
        cols.append(ColumnLineage(name=col.name, parent_model="_from_clause", source_columns=[col]))
    return cols


class LineageTask(GraphRunnableTask):
    def get_node_selector(self):
        if self.manifest is None or self.graph is None:
            raise InternalException("manifest and graph must be set to get perform node selection")
        if self.resource_types == [NodeType.Test]:
            return TestSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
            )
        else:
            return ResourceTypeSelector(
                graph=self.graph,
                manifest=self.manifest,
                previous_state=self.previous_state,
                resource_types=self.resource_types,
            )

    
    def run(self):
        ManifestTask._runtime_initialize(self)

        # Get the project root path and manifest path
        root_path = Path("/Users/iknox/Projects/hackathon-CLL/sources/wizards")
        manifest_path = root_path / "target" / "manifest.json"
        graph_path = root_path / "target" / "graph.gpickle"

        # Load files
        project_graph = nx.read_gpickle(graph_path)
        with open(manifest_path) as manifest_fh:
            manifest = json.load(manifest_fh)

        # Instantiate env and get node list 
        nodes = nx.topological_sort(project_graph)
        env = Env(models={})

        # Iterate nodes and process
        for node_name in nodes:
            if node_name[0:7] == "source.":
                node = manifest["sources"][node_name]
                rel = manifest["sources"][node_name]["relation_name"]
                
                source_cols = []                
                for col in node["columns"].keys():
                    source_cols.append(ColumnLineage(
                        name=col,
                        parent_model=rel,
                        source_columns=[]
                    )
                )
                
                node_lineage = NodeLineage(
                    name=rel,
                    columns=source_cols
                )

                env.models[rel] = node_lineage

            if node_name[0:6] == "model.":
                sql = manifest["nodes"][node_name]["compiled_code"]
                rel = manifest["nodes"][node_name]["relation_name"]
                

                parsed_query = sqlglot.parse(sql)

                if isinstance(parsed_query[0], exp.Select):
                    select_statment = parsed_query[0]
                    internal_lineage = get_lineage_by_select(env, select_statment, rel)
                    final_lineage = collapse_lineage(internal_lineage)
                    env.models[rel] = final_lineage
                
                    breakpoint()
                    print(final_lineage)


        
        
        

