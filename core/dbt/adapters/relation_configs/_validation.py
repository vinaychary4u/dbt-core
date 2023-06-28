from dataclasses import dataclass
from typing import Optional, Set

from dbt.exceptions import DbtRuntimeError


@dataclass(frozen=True, eq=True, unsafe_hash=True)
class RelationConfigValidationRule:
    """
    A validation rule consists of two parts:
        - validation_check: the thing that should be True
        - validation_error: the error to raise in the event the validation check is False
    """

    validation_check: bool
    validation_error: Optional[DbtRuntimeError]

    @property
    def default_error(self):
        """
        This is a built-in stock error message. It may suffice in that it will raise an error for you, but
        you should likely supply one in the rule that is more descriptive. This is akin to raising `Exception`.

        Returns:
            a stock error message
        """
        return DbtRuntimeError(
            "There was a validation error in preparing this relation config."
            "No additional context was provided by this adapter."
        )


@dataclass(frozen=True)
class RelationConfigValidationMixin:
    def __post_init__(self):
        self.run_validation_rules()

    @property
    def validation_rules(self) -> Set[RelationConfigValidationRule]:
        """
        A set of validation rules to run against the object upon creation.

        A validation rule is a combination of a validation check (bool) and an optional error message.

        This defaults to no validation rules if not implemented. It's recommended to override this with values,
        but that may not always be necessary.

        Returns: a set of validation rules
        """
        return set()

    def run_validation_rules(self):
        for validation_rule in self.validation_rules:
            try:
                assert validation_rule.validation_check
            except AssertionError:
                if validation_rule.validation_error:
                    raise validation_rule.validation_error
                else:
                    raise validation_rule.default_error
        self.run_child_validation_rules()

    def run_child_validation_rules(self):
        for attr_value in vars(self).values():
            if hasattr(attr_value, "validation_rules"):
                attr_value.run_validation_rules()
            if isinstance(attr_value, set):
                for member in attr_value:
                    if hasattr(member, "validation_rules"):
                        member.run_validation_rules()
