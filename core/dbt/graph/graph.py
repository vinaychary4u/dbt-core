from typing import Set, Iterable, Iterator, Optional, NewType
from abc import ABC, abstractmethod
from itertools import product

UniqueId = NewType("UniqueId", str)


class Graph(ABC):
    """A wrapper around the networkx graph that understands SelectionCriteria
    and how they interact with the graph.
    """

    def __init__(self, graph):
        self.graph = graph

    def nodes(self) -> Set[UniqueId]:
        return set(self.graph.nodes())

    def edges(self):
        return self.graph.edges()

    def __iter__(self) -> Iterator[UniqueId]:
        return iter(self.graph.nodes())

    @abstractmethod
    def in_degree():
        pass

    @abstractmethod
    def ancestors(self, node: UniqueId, max_depth: Optional[int] = None) -> Set[UniqueId]:
        """Returns all nodes having a path to `node` in `graph`"""
        pass

    @abstractmethod
    def descendants(self, node: UniqueId, max_depth: Optional[int] = None) -> Set[UniqueId]:
        """Returns all nodes reachable from `node` in `graph`"""
        pass

    def select_childrens_parents(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        ancestors_for = self.select_children(selected) | selected
        return self.select_parents(ancestors_for) | ancestors_for

    def select_children(
        self, selected: Set[UniqueId], max_depth: Optional[int] = None
    ) -> Set[UniqueId]:
        descendants: Set[UniqueId] = set()
        for node in selected:
            descendants.update(self.descendants(node, max_depth))
        return descendants

    def select_parents(
        self, selected: Set[UniqueId], max_depth: Optional[int] = None
    ) -> Set[UniqueId]:
        ancestors: Set[UniqueId] = set()
        for node in selected:
            ancestors.update(self.ancestors(node, max_depth))
        return ancestors

    def select_successors(self, selected: Set[UniqueId]) -> Set[UniqueId]:
        successors: Set[UniqueId] = set()
        for node in selected:
            successors.update(self.graph.successors(node))
        return successors

    @abstractmethod
    def get_subset_graph(self, selected: Iterable[UniqueId]) -> "Graph":
        """Create and return a new graph that is a shallow copy of the graph,
        but with only the nodes in include_nodes. Transitive edges across
        removed nodes are preserved as explicit new edges.
        """
        pass

    @abstractmethod
    def subgraph(self, nodes: Iterable[UniqueId]) -> "Graph":
        pass

    def get_dependent_nodes(self, node: UniqueId):
        return self.descendants(node)
