class LanguageProvider:

    # def compile(self, code: str) -> ParsedNode:
    def compile(self, code: str) -> str:
        """
        Compile a given block into a ParsedNode.
        """
        raise NotImplementedError("compile")


class IbisProvider(LanguageProvider):
    def __init__(self) -> None:
        # TODO: Uncomment when dbt-ibis is released
        # if not dbt_ibis:
        #     raise ImportError(
        #         "dbt_ibis is required and not found; try running `pip install dbt-ibis`"
        #     )
        pass

    def compile(self, code: str, context) -> str:
        from .dbt_ibis import _dbt_ibis as dbt_ibis

        return dbt_ibis.compile(code, context)
