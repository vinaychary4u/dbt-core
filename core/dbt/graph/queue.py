from abc import ABC, abstractmethod
import threading

from queue import PriorityQueue
from typing import Set, Optional

from .graph import Graph, UniqueId
from dbt.contracts.graph.compiled import GraphMemberNode
from dbt.contracts.graph.manifest import Manifest


class GraphQueue(ABC):
    """A fancy queue that is backed by the dependency graph.
    Note: this will mutate input!

    This queue is thread-safe for `mark_done` calls, though you must ensure
    that separate threads do not call `.empty()` or `__len__()` and `.get()` at
    the same time, as there is an unlocked race!
    """

    def __init__(self, graph: Graph, manifest: Manifest, selected: Set[UniqueId]):
        self.manifest = manifest
        self._selected = selected
        # store the queue as a priority queue.
        self.inner: PriorityQueue = PriorityQueue()
        # things that have been popped off the queue but not finished
        # and worker thread reservations
        self.in_progress: Set[UniqueId] = set()
        # things that are in the queue
        self.queued: Set[UniqueId] = set()
        # this lock controls most things
        self.lock = threading.Lock()
        # populate the initial queue
        self.find_new_additions()
        # awaits after task end
        self.some_task_done = threading.Condition(self.lock)

    def _mark_in_progress(self, node_id: UniqueId) -> None:
        """Mark the node as 'in progress'.
        Callers must hold the lock.
        :param str node_id: The node ID to mark as in progress.
        """
        self.queued.remove(node_id)
        self.in_progress.add(node_id)

    def join(self) -> None:
        """Join the queue. Blocks until all tasks are marked as done.
        Make sure not to call this before the queue reports that it is empty.
        """
        self.inner.join()

    def get_selected_nodes(self) -> Set[UniqueId]:
        return self._selected.copy()

    def get(self, block: bool = True, timeout: Optional[float] = None) -> GraphMemberNode:
        """Get a node off the inner priority queue. By default, this blocks.
        This takes the lock, but only for part of it.
        :param block: If True, block until the inner queue has data
        :param timeout: If set, block for timeout seconds waiting for data.
        :return: The node as present in the manifest.
        See `queue.PriorityQueue` for more information on `get()` behavior and
        exceptions.
        """
        _, node_id = self.inner.get(block=block, timeout=timeout)
        with self.lock:
            self._mark_in_progress(node_id)
        return self.manifest.expect(node_id)

    def __len__(self) -> int:
        """The length of the queue is the number of tasks left for the queue to
        give out, regardless of where they are. Incomplete tasks are not part
        of the length.

        This takes the lock.
        """
        with self.lock:
            return self.get_node_num() - len(self.in_progress)

    def empty(self) -> bool:
        """The graph queue is 'empty' if it all remaining nodes in the graph
        are in progress.

        This takes the lock.
        """
        return len(self) == 0

    def _already_known(self, node: UniqueId) -> bool:
        """Decide if a node is already known (either handed out as a task, or
        in the queue).

        Callers must hold the lock.

        :param str node: The node ID to check
        :returns bool: If the node is in progress/queued.
        """
        return node in self.in_progress or node in self.queued

    def mark_done(self, node_id: UniqueId) -> None:
        """Given a node's unique ID, mark it as done.
        This method takes the lock.
        :param str node_id: The node ID to mark as complete.
        """
        with self.lock:
            self.in_progress.remove(node_id)
            self.remove_node_from_graph(node_id)
            self.find_new_additions()
            self.inner.task_done()
            self.some_task_done.notify_all()

    def wait_until_something_was_done(self) -> int:
        """Block until a task is done, then return the number of unfinished
        tasks.
        """
        with self.lock:
            self.some_task_done.wait()
            return self.inner.unfinished_tasks

    @abstractmethod
    def find_new_additions(self):
        pass

    @abstractmethod
    def remove_node_from_graph(self, node_id):
        pass

    @abstractmethod
    def get_node_num(self):
        pass
