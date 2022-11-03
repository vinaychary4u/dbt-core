import dbt.dataclass_schema
from typing import Optional


def validator_error_message(exc):
    """Given a dbt.dataclass_schema.ValidationError (which is basically a
    jsonschema.ValidationError), return the relevant parts as a string
    """
    if not isinstance(exc, dbt.dataclass_schema.ValidationError):
        return str(exc)
    path = "[%s]" % "][".join(map(repr, exc.relative_path))
    return "at path {}: {}".format(path, exc.message)


def get_not_found_or_disabled_msg(
    original_file_path,
    unique_id,
    resource_type_title,
    target_name: str,
    target_kind: str,
    target_package: Optional[str] = None,
    disabled: Optional[bool] = None,
) -> str:
    if disabled is None:
        reason = "was not found or is disabled"
    elif disabled is True:
        reason = "is disabled"
    else:
        reason = "was not found"

    target_package_string = ""
    if target_package is not None:
        target_package_string = "in package '{}' ".format(target_package)

    return "{} '{}' ({}) depends on a {} named '{}' {}which {}".format(
        resource_type_title,
        unique_id,
        original_file_path,
        target_kind,
        target_name,
        target_package_string,
        reason,
    )


# TODO: temp fix for mypy/proto sanity - combine later
def get_not_found_or_disabled_msg_2(
    original_file_path,
    unique_id,
    resource_type_title,
    target_name: str,
    target_kind: str,
    disabled: str,
    target_package: Optional[str] = None,
) -> str:
    if disabled == "None":
        reason = "was not found or is disabled"
    elif disabled == "True":
        reason = "is disabled"
    else:
        reason = "was not found"

    target_package_string = ""
    if target_package is not None:
        target_package_string = "in package '{}' ".format(target_package)

    return "{} '{}' ({}) depends on a {} named '{}' {}which {}".format(
        resource_type_title,
        unique_id,
        original_file_path,
        target_kind,
        target_name,
        target_package_string,
        reason,
    )
