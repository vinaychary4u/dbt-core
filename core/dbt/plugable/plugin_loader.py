from importlib.metadata import entry_points
from typing import Union, List


def get_plugable(group_name: str, plugin_name: Union[List[str], str] = "default") -> any:
    plugins = entry_points(group=group_name)
    return plugins[plugin_name].load()
