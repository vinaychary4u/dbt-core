from typing import List
from dbt.dbt_semantic.references import EntityElementReference


from dbt.contracts.graph.nodes import Entity
from dbt.dbt_semantic.objects.user_configured_model import UserConfiguredModel
from dbt.dbt_semantic.validations.validator_helpers import (
    EntityContext,
    EntityElementContext,
    EntityElementType,
    ModelValidationRule,
    ValidationError,
    ValidationIssueType,
    validate_safely,
)

# A non-exaustive tuple of reserved keywords
# This list was created by running an intersection of keywords for redshift,
# postgres, bigquery, and snowflake
RESERVED_KEYWORDS = (
    "and",
    "as",
    "create",
    "distinct",
    "for",
    "from",
    "full",
    "having",
    "in",
    "inner",
    "into",
    "is",
    "join",
    "left",
    "like",
    "natural",
    "not",
    "null",
    "on",
    "or",
    "right",
    "select",
    "union",
    "using",
    "where",
    "with",
)


class ReservedKeywordsRule(ModelValidationRule):
    """Check that any element that ends up being selected by name (instead of expr) isn't a commonly reserved keyword.

    Note: This rule DOES NOT catch all keywords. That is because keywords are
    engine specific, and semantic validations are not engine specific. I.e. if
    you change your underlying data warehouse engine, semantic validations
    should still pass, but your data warehouse validations might fail. However,
    data warehouse validations are slow in comparison to semantic validation
    rules. Thus this rule is intended to catch words that are reserved keywords
    in all supported engines and to fail fast. E.g., `USER` is a reserved keyword
    in Redshift but not in all other supported engines. Therefore if one is
    using Redshift and sets a dimension name to `user`, the config would pass
    this rule, but would then fail Data Warehouse Validations.
    """

    @staticmethod
    def _validate_entity_sub_elements(entity: Entity) -> List[ValidationIssueType]:
        issues: List[ValidationIssueType] = []

        if entity.dimensions:
            for dimension in entity.dimensions:
                if dimension.name.lower() in RESERVED_KEYWORDS:
                    issues.append(
                        ValidationError(
                            context=EntityElementContext(
                                entity_element=EntityElementReference(
                                    entity_name=entity.name, name=dimension.name
                                ),
                                element_type=EntityElementType.DIMENSION,
                            ),
                            message=f"'{dimension.name}' is an SQL reserved keyword, and thus cannot be used as a dimension 'name'.",
                        )
                    )

        if entity.identifiers:
            for identifier in entity.identifiers:
                if identifier.is_composite:
                    msg = "'{name}' is an SQL reserved keyword, and thus cannot be used as a sub-identifier 'name'"
                    names = [sub_ident.name for sub_ident in identifier.identifiers if sub_ident.name is not None]
                else:
                    msg = "'{name}' is an SQL reserved keyword, and thus cannot be used as an identifier 'name'"
                    names = [identifier.name]

                for name in names:
                    if name.lower() in RESERVED_KEYWORDS:
                        issues.append(
                            ValidationError(
                                context=EntityElementContext(
                                    entity_element=EntityElementReference(
                                        entity_name=entity.name, name=identifier.name
                                    ),
                                    element_type=EntityElementType.IDENTIFIER,
                                ),
                                message=msg.format(name=name),
                            )
                        )

        if entity.measures:
            for measure in entity.measures:
                if measure.name.lower() in RESERVED_KEYWORDS:
                    issues.append(
                        ValidationError(
                            context=EntityElementContext(
                                entity_element=EntityElementReference(
                                    entity_name=entity.name, name=measure.name
                                ),
                                element_type=EntityElementType.MEASURE,
                            ),
                            message=f"'{measure.name}' is an SQL reserved keyword, and thus cannot be used as an measure 'name'.",
                        )
                    )

        return issues

    @classmethod
    def _validate_entities(cls, model: UserConfiguredModel) -> List[ValidationIssueType]:
        """Checks names of objects that are not nested."""
        issues: List[ValidationIssueType] = []

        for entity in model.entities:
            issues += cls._validate_entity_sub_elements(entity=entity)

        return issues


    @classmethod
    def validate_model(cls, model: UserConfiguredModel) -> List[ValidationIssueType]:  # noqa: D
        return cls._validate_entities(model=model)