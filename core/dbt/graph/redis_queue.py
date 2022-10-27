import networkx as nx  # type: ignore

from typing import Set

from .graph import UniqueId
from .queue import GraphQueue

from dbt.contracts.graph.manifest import Manifest


class RedisGraphQueue(GraphQueue):
    def __init__(self, graph: nx.DiGraph, manifest: Manifest, selected: Set[UniqueId]) -> None:
        # create entry on run_results,
        # create new graph in redis from graph
        pass

    def find_new_additions(self) -> None:
        """Find any nodes in the graph that need to be added to the internal
        queue and add them.
        """

    def remove_node_from_graph(self, node_id):
        pass

    def get_node_num(self):
        pass
