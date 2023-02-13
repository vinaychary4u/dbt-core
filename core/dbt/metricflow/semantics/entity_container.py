from abc import ABC, abstractmethod
from typing import Generic, List, TypeVar, Dict

from dbt.contracts.graph.nodes import Entity

T = TypeVar("T")


class EntityContainer(ABC, Generic[T]):  # noqa: D
    @abstractmethod
    def get(self, data_source_name: str) -> T:  # noqa: D
        pass

    @abstractmethod
    def values(self) -> List[T]:  # noqa: D
        pass

    @abstractmethod
    def keys(self) -> List[str]:  # noqa: D
        pass

    @abstractmethod
    def __contains__(self, item: str) -> bool:  # noqa: D
        pass

    @abstractmethod
    def put(self, key: str, value: T) -> None:  # noqa: D
        pass


class DataSourceContainer(EntityContainer[Entity]):  # noqa: D
    def __init__(self, data_sources: List[Entity]) -> None:  # noqa: D
        self._data_source_index: Dict[str, Entity] = {data_source.name: data_source for data_source in data_sources}

    def get(self, data_source_name: str) -> Entity:  # noqa: D
        return self._data_source_index[data_source_name]

    def values(self) -> List[Entity]:  # noqa: D
        return list(self._data_source_index.values())

    def put(self, key: str, value: T) -> None:  # noqa: D
        raise TypeError("Cannot call put on static data source container. Data sources are fixed on init")

    def _put(self, data_source: Entity) -> None:
        """Dont use this unless you mean it (ie in tests). This is supposed to be static"""
        self._data_source_index[data_source.name] = data_source

    def keys(self) -> List[str]:  # noqa: D
        return list(self._data_source_index.keys())

    def __contains__(self, item: str) -> bool:  # noqa: D
        return item in self._data_source_index