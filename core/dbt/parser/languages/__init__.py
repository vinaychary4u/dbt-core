from .provider import LanguageProvider  # noqa
from .jinja_sql import JinjaSQLProvider  # noqa
from .python import PythonProvider  # noqa

# TODO: how to make this discovery/registration pluggable?
from .prql import PrqlProvider  # noqa
from .ibis import IbisProvider  # noqa


def get_language_providers():
    return LanguageProvider.__subclasses__()


def get_language_names():
    return [provider.name() for provider in get_language_providers()]


def get_file_extensions():
    return [provider.file_ext() for provider in get_language_providers()]


def get_language_provider_by_name(language_name: str) -> LanguageProvider:
    return next(
        iter(provider for provider in get_language_providers() if provider.name() == language_name)
    )
