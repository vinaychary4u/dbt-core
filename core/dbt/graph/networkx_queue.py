import networkx as nx  # type: ignore

from typing import Dict, Set, List, Generator

from .graph import UniqueId
from .queue import GraphQueue
from dbt.contracts.graph.parsed import ParsedSourceDefinition, ParsedExposure, ParsedMetric
from dbt.contracts.graph.manifest import Manifest
from dbt.node_types import NodeType


class NetworkxGraphQueue(GraphQueue):
    """A fancy queue that is backed by the dependency graph.
    Note: this will mutate input!
    This queue is thread-safe for `mark_done` calls, though you must ensure
    that separate threads do not call `.empty()` or `__len__()` and `.get()` at
    the same time, as there is an unlocked race!
    """

    def __init__(self, graph: nx.DiGraph, manifest: Manifest, selected: Set[UniqueId]) -> None:
        # store the 'score' of each node as a number. Lower is higher priority.
        self._scores = self._get_scores(graph)
        self.graph = graph
        super().__init__(graph, manifest, selected)

    def _include_in_cost(self, node_id: UniqueId) -> bool:
        node = self.manifest.expect(node_id)
        if node.resource_type != NodeType.Model:
            return False
        # must be a Model - tell mypy this won't be a Source or Exposure or Metric
        assert not isinstance(node, (ParsedSourceDefinition, ParsedExposure, ParsedMetric))
        if node.is_ephemeral:
            return False
        return True

    @staticmethod
    def _grouped_topological_sort(
        graph: nx.DiGraph,
    ) -> Generator[List[str], None, None]:
        """Topological sort of given graph that groups ties.
        Adapted from `nx.topological_sort`, this function returns a topo sort of a graph however
        instead of arbitrarily ordering ties in the sort order, ties are grouped together in
        lists.
        Args:
            graph: The graph to be sorted.
        Returns:
            A generator that yields lists of nodes, one list per graph depth level.
        """
        indegree_map = {v: d for v, d in graph.in_degree() if d > 0}
        zero_indegree = [v for v, d in graph.in_degree() if d == 0]

        while zero_indegree:
            yield zero_indegree
            new_zero_indegree = []
            for v in zero_indegree:
                for _, child in graph.edges(v):
                    indegree_map[child] -= 1
                    if not indegree_map[child]:
                        new_zero_indegree.append(child)
            zero_indegree = new_zero_indegree

    def _get_scores(self, graph: nx.DiGraph) -> Dict[str, int]:
        """Scoring nodes for processing order.
        Scores are calculated by the graph depth level. Lowest score (0) should be processed first.
        Args:
            graph: The graph to be scored.
        Returns:
            A dictionary consisting of `node name`:`score` pairs.
        """
        # split graph by connected subgraphs
        subgraphs = (graph.subgraph(x) for x in nx.connected_components(nx.Graph(graph)))

        # score all nodes in all subgraphs
        scores = {}
        for subgraph in subgraphs:
            grouped_nodes = self._grouped_topological_sort(subgraph)
            for level, group in enumerate(grouped_nodes):
                for node in group:
                    scores[node] = level

        return scores

    def find_new_additions(self) -> None:
        """Find any nodes in the graph that need to be added to the internal
        queue and add them.
        """
        for node, in_degree in self.graph.in_degree():
            if not self._already_known(node) and in_degree == 0:
                self.inner.put((self._scores[node], node))
                self.queued.add(node)

    def remove_node_from_graph(self, node_id):
        self.graph.remove_node(node_id)

    def get_node_num(self):
        return len(self.graph.nodes())
