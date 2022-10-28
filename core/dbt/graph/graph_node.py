from dataclasses import dataclass


@dataclass
class GraphNode:
    name: str
    in_degree: int
    status: str
    update_date: int
    start_date: int
