from dataclasses import asdict
from datetime import datetime
import networkx as nx  # type: ignore

from typing import Set
import redis
import time
from .graph_node import GraphNode
from redisgraph import Node, Edge, Graph

from .graph import UniqueId
from .queue import GraphQueue

from dbt.contracts.graph.manifest import Manifest


class RedisGraphQueue(GraphQueue):
    # TODO this skips ephemeral model

    def __init__(self, graph: nx.DiGraph, manifest: Manifest, selected: Set[UniqueId]) -> None:
        # create entry on run_results,
        # create new graph in redis from graph
        self.r = redis.Redis(host="localhost", port=6379)
        self.name = int(time.time())
        self.redis_graph = Graph(self.name, self.r)
        nodes = {}
        for node_name, in_degree in graph.in_degree():
            start_time = int(datetime.now().timestamp())
            properties = GraphNode(
                name=node_name,
                in_degree=in_degree,
                status="created",
                update_date=start_time,
                start_date=start_time
            )
            curr_node = Node(
                label="node",
                properties=asdict(properties),
            )
            self.redis_graph.add_node(curr_node)
            nodes[node_name] = curr_node
        for child_name, father_name in graph.edges():
            edge = Edge(nodes[child_name], "is_child_of", nodes[father_name])
            self.redis_graph.add_edge(edge)
        self.redis_graph.commit()
        self.node_count = len(nodes)
        super().__init__(graph, manifest, selected)

    def find_new_additions(self) -> None:
        """Find any nodes in the graph that need to be added to the internal
        queue and add them.
        """
        query = """MATCH (p:node {in_degree :0, status: 'created'})
           RETURN p.name"""
        result = self.redis_graph.query(query)
        for name in result.result_set:
            node = name[0]
            if not self._already_known(node):
                self.inner.put((0, node))
                self.queued.add(node)
        query = """MATCH (p:node {in_degree :0, status: 'created'})
           set p.status = 'queued'"""
        self.redis_graph.query(query)

    def remove_node_from_graph(self, node_id):
        # find all childnode of current
        self.node_count -= 1
        query = f"""MATCH (p:node {{name: '{node_id}'}})- [v:is_child_of] ->(c:node)
           RETURN c.name, c.in_degree"""
        result = self.redis_graph.query(query)
        # reduce the in degree of them
        for model_name, in_degree in result.result_set:
            query = (
                f"""MATCH (p:node {{name: '{model_name}'}}) SET p.in_degree = {in_degree - 1}"""
            )
            self.redis_graph.query(query)
        # mark current node
        current_time = int(datetime.now().timestamp())
        query = f"""MATCH (p:node {{name: '{node_id}'}}) SET p.status = 'done', p.update_date = {current_time}"""
        self.redis_graph.query(query)

    def get_node_num(self):
        return self.node_count
