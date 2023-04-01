from dbt.clients import jinja
from dbt.context.context_config import ContextConfig
from dbt.parser.languages.provider import LanguageProvider
from dbt.contracts.graph.nodes import ManifestNode

from typing import Dict, Any


class JinjaSQLProvider(LanguageProvider):
    @classmethod
    def name(self) -> str:
        return "sql"

    @classmethod
    def update_context(
        cls, node: Any, config: ContextConfig, context: Dict[str, Any]
    ) -> Dict[str, Any]:
        # this goes through the process of rendering, but we don't keep the rendered result
        # the goal is to capture macros + update context as side effect
        jinja.get_rendered(node.raw_code, context, node, capture_macros=True)
        return context

    @classmethod
    def get_compiled_code(self, node: ManifestNode, context: Dict[str, Any]) -> str:
        compiled_code = jinja.get_rendered(
            node.raw_code,
            context,
            node,
        )
        return compiled_code

    @classmethod
    def needs_compile_time_connection(self) -> bool:
        return True
