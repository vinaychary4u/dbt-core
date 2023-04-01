from __future__ import annotations
from typing import Dict, Tuple, List, Any
import abc

# for type hints
from dbt.contracts.graph.nodes import RefArgs, ManifestNode
from dbt.context.providers import RelationProxy
from dbt.context.context_config import ContextConfig

# TODO rework these types now that 'ref' accepts a keyword argument ('v' or 'version')
dbt_function_calls = List[Tuple[str, List[str], Dict[str, Any]]]
references_type = Dict[str, Dict[Tuple[str, ...], RelationProxy]]


class LanguageProvider(metaclass=abc.ABCMeta):
    """
    A LanguageProvider is a class that can parse & compile a given language.
    """

    @classmethod
    def name(self) -> str:
        return ""

    @classmethod
    def file_ext(self) -> str:
        return f".{self.name()}"

    @classmethod
    def compiled_language(self) -> str:
        return self.name()

    @classmethod
    @abc.abstractmethod
    # TODO add type hints
    def extract_dbt_function_calls(self, node: Any) -> dbt_function_calls:
        """
        List all dbt function calls (ref, source, config) and their args/kwargs
        """
        raise NotImplementedError("extract_dbt_function_calls")

    @classmethod
    def validate_raw_code(self, node: Any) -> None:
        pass

    @classmethod
    def update_context(
        cls, node: Any, config: ContextConfig, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        dbt_function_calls = cls.extract_dbt_function_calls(node)
        config_keys_used = []
        for (func, args, kwargs) in dbt_function_calls:
            if func == "get":
                config_keys_used.append(args[0])
                continue

            context[func](*args, **kwargs)
        if config_keys_used:
            # this is being used in macro build_config_dict
            context["config"](config_keys_used=config_keys_used)
        return context

    @classmethod
    @abc.abstractmethod
    def needs_compile_time_connection(self) -> bool:
        """
        Does this modeling language support introspective queries (requiring a database connection)
        at compile time?
        """
        raise NotImplementedError("needs_compile_time_connection")

    @classmethod
    def get_resolved_references(
        self, node: ManifestNode, context: Dict[str, Any]
    ) -> references_type:
        resolved_references: references_type = {
            "sources": {},
            "refs": {},
        }
        # TODO: do we need to support custom 'ref' + 'source' resolution logic for non-JinjaSQL languages?
        # i.e. user-defined 'ref' + 'source' macros -- this approach will not work for that
        refs: List[RefArgs] = node.refs
        sources: List[List[str]] = node.sources
        for ref in refs:
            resolved_ref: RelationProxy = context["ref"](*ref)
            resolved_references["refs"].update({tuple(ref): resolved_ref})
        for source in sources:
            resolved_src: RelationProxy = context["source"](*source)
            resolved_references["sources"].update({tuple(source): resolved_src})
        return resolved_references

    @classmethod
    @abc.abstractmethod
    def get_compiled_code(self, node: ManifestNode, context: Dict[str, Any]) -> str:
        """
        For a given ManifestNode, return its compiled code.
        """
        raise NotImplementedError("get_compiled_code")
