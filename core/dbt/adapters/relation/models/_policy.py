from abc import ABC
from dataclasses import dataclass
from typing import Optional, OrderedDict

from dbt.contracts.relation import Policy, ComponentName


@dataclass
class IncludePolicy(Policy, ABC):
    pass


@dataclass
class QuotePolicy(Policy, ABC):
    pass


class RenderPolicy:
    def __init__(
        self,
        quote_policy: QuotePolicy = QuotePolicy(),
        include_policy: IncludePolicy = IncludePolicy(),
        quote_character: str = '"',
        delimiter: str = ".",
    ):
        self.quote_policy = quote_policy
        self.include_policy = include_policy
        self.quote_character = quote_character
        self.delimiter = delimiter

    def part(self, component: ComponentName, value: str) -> Optional[str]:
        """
        Apply the include and quote policy to the value so that it may be rendered in a template.

        Args:
            component: the component to be referenced in `IncludePolicy` and `QuotePolicy`
            value: the value to be rendered

        Returns:
            a policy-compliant value
        """
        # this is primarily done to make it easy to create the backup and intermediate names, e.g.
        # name = "my_view", backup_name = "my_view"__dbt_backup, rendered_name = "my_view__dbt_backup"
        unquoted_value = value.replace(self.quote_character, "")

        # if it should be included and quoted, then wrap it in quotes as-is
        if self.include_policy.get_part(component) and self.quote_policy.get_part(component):
            rendered_value = f"{self.quote_character}{unquoted_value}{self.quote_character}"

        # if it should be included without quotes, then apply `lower()` to make it case-insensitive
        elif self.include_policy.get_part(component):
            rendered_value = unquoted_value.lower()

        # if it should not be included, return `None`, so it gets excluded in `render`
        else:
            rendered_value = None

        return rendered_value

    def full(self, parts: OrderedDict[ComponentName, str]) -> str:
        """
        Apply `Render.part` to each part and then concatenate in order.

        Args:
            parts: an ordered dictionary mapping ComponentName to value

        Returns:
            a fully rendered path ready for a jinja template
        """
        rendered_parts = [self.part(*part) for part in parts.items()]
        rendered_path = self.delimiter.join(part for part in rendered_parts if part is not None)
        return rendered_path
