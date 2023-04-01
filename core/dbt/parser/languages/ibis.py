import ibis
import ast

from dbt.parser.languages.provider import LanguageProvider, dbt_function_calls
from dbt.parser.languages.python import PythonParseVisitor
from dbt.contracts.graph.nodes import ManifestNode

from dbt.exceptions import PythonParsingError

from typing import Any, Dict


class IbisProvider(LanguageProvider):
    @classmethod
    def name(self) -> str:
        return "ibis"

    # TODO: how can we differentiate from python models?
    # can we support IPython-style magic, e.g. `%ibis`, at the top of the file?
    @classmethod
    def file_ext(self) -> str:
        return ".py"

    @classmethod
    def compiled_language(self) -> str:
        return "sql"

    @classmethod
    def validate_raw_code(self, node) -> None:
        # don't require the 'model' function for now
        pass

    @classmethod
    def extract_dbt_function_calls(self, node) -> dbt_function_calls:
        """
        List all references (refs, sources, configs) in a given block.
        """
        try:
            tree = ast.parse(node.raw_code, filename=node.original_file_path)
        except SyntaxError as exc:
            raise PythonParsingError(exc, node=node) from exc

        # Only parse if AST tree has instructions in body
        if tree.body:
            # don't worry about the 'model' function for now
            # dbt_validator = PythonValidationVisitor()
            # dbt_validator.visit(tree)
            # dbt_validator.check_error(node)

            dbt_parser = PythonParseVisitor(node)
            dbt_parser.visit(tree)
            return dbt_parser.dbt_function_calls
        else:
            return []

    @classmethod
    def needs_compile_time_connection(self) -> bool:
        # TODO: this is technically true, but Ibis won't actually use dbt's connection, it will make its own
        return True

    @classmethod
    def get_compiled_code(self, node: ManifestNode, context: Dict[str, Any]) -> str:
        resolved_references = self.get_resolved_references(node, context)

        def ref(*args, dbt_load_df_function):
            refs = resolved_references["refs"]
            key = tuple(args)
            return dbt_load_df_function(refs[key])

        def source(*args, dbt_load_df_function):
            sources = resolved_references["sources"]
            key = tuple(args)
            return dbt_load_df_function(sources[key])

        config_dict = {}
        for key in node.config.get("config_keys_used", []):
            value = node.config[key]
            config_dict.update({key: value})

        class config:
            def __init__(self, *args, **kwargs):
                pass

            @staticmethod
            def get(key, default=None):
                return config_dict.get(key, default)

        class this:
            """dbt.this() or dbt.this.identifier"""

            database = node.database
            schema = node.schema
            identifier = node.identifier

            def __repr__(self):
                return node.relation_name

        class dbtObj:
            def __init__(self, load_df_function) -> None:
                self.source = lambda *args: source(*args, dbt_load_df_function=load_df_function)
                self.ref = lambda *args: ref(*args, dbt_load_df_function=load_df_function)
                self.config = config
                self.this = this()
                # self.is_incremental = TODO

        # https://ibis-project.org/docs/dev/backends/PostgreSQL/#ibis.backends.postgres.Backend.do_connect
        # TODO: this would need to live in the adapter somehow
        target = context["target"]
        con = ibis.postgres.connect(
            database=target["database"],
            user=target["user"],
        )

        # use for dbt.ref(), dbt.source(), etc
        dbt = dbtObj(con.table)  # noqa

        # TODO: this is unsafe in so many ways
        exec(node.raw_code)
        compiled = str(eval(f"ibis.{context['target']['type']}.compile(model)"))

        return compiled
