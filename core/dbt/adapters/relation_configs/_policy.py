from abc import ABC
from dataclasses import dataclass
from typing import Optional, OrderedDict

from dbt.contracts.relation import ComponentName, Policy


@dataclass
class IncludePolicy(Policy, ABC):
    pass


@dataclass
class QuotePolicy(Policy, ABC):
    @property
    def quote_character(self) -> str:
        """This is property to appeal to the `Policy` serialization."""
        return '"'


"""
It's advised to create your own adapter-specific version of these functions to pipe in the policies
that are specific to your adapter and for easier maintenance. This can be done easily with `functools.partial`.
See `dbt/adapters/postgres/relation_configs/policy.py` for an example.
"""


def conform_part(
    component: ComponentName, value: str, quote_policy: QuotePolicy = QuotePolicy()
) -> Optional[str]:
    """
    Apply the quote policy to the value so that it may be stored on the config object.

    *Note: Parts get quoted as part of methods like list_relations. As a result, a quote policy
    of `True` just means "leave it alone", whereas a quote policy of `False` means make it case-insensitive,
    which in this case is `str.lower()`. This differs from `render_part` which should only be used
    for preparing templates. In that case, the quote character is used.

    It's advised to create your own adapter-specific version to pipe in the policies for easier maintenance.
    See `dbt/adapters/postgres/relation_configs/policy.py` for an example.

    Args:
        component: the component of the policy to apply
        value: the value to which the policies should be applied
        quote_policy: the quote policy for the adapter

    Returns:
        a policy-compliant value
    """
    if quote_policy.get_part(component):
        return value
    return value.lower()


def render_part(
    component: ComponentName,
    value: str,
    quote_policy: QuotePolicy = QuotePolicy(),
    include_policy: IncludePolicy = IncludePolicy(),
) -> Optional[str]:
    """
    Apply the include and quote policy to the value so that it may be rendered in a template.

    *Note: This differs from `conform_part` in that the quote character actually gets used and
    the include policy gets applied.
    Additionally, there may be times when `value` shows up already quoted, if that's the case, these
    characters are removed so that `value` does not wind up double-quoted.

    It's advised to create your own adapter-specific version to pipe in the policies for easier maintenance.
    See `dbt/adapters/postgres/relation_configs/policy.py` for an example.

    Args:
        component: the component of the policy to apply
        value: the value to which the policies should be applied
        quote_policy: the quote policy for the adapter
        include_policy: the include policy for the adapter

    Returns:
        a policy-compliant value
    """
    quote = quote_policy.quote_character
    if include_policy.get_part(component):
        if quote_policy.get_part(component):
            return f"{quote}{value.replace(quote, '')}{quote}"
        return value.lower()
    return None


def render(
    parts: OrderedDict[ComponentName, str],
    quote_policy: QuotePolicy = QuotePolicy(),
    include_policy: IncludePolicy = IncludePolicy(),
    delimiter: str = ".",
) -> str:
    """
    This does the same thing as `cls.render_part()`, but all at once.

    We need to make sure we join the parts in the correct order, including scenarios where we don't
    receive all the components, e.g. someone needs a fully qualified schema (hence no identifier).

    It's advised to create your own adapter-specific version to pipe in the policies for easier maintenance.
    See `dbt/adapters/postgres/relation_configs/policy.py` for an example.

    Args:
        parts: an ordered dictionary mapping ComponentName to value, provide the dictionary in the order in which
        you want the parts joined
        quote_policy: the quote policy for the adapter
        include_policy: the include policy for the adapter
        delimiter: the delimiter to use between parts

    Returns:
        a fully rendered path ready for a jinja template
    """
    rendered_parts = [
        render_part(component, value, quote_policy, include_policy)
        for component, value in parts.items()
    ]
    rendered_path = delimiter.join(part for part in rendered_parts if part is not None)
    return rendered_path
