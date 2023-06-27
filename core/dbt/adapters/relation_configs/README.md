# Relation Configs
This package serves as an initial abstraction for managing the inspection of existing relations and determining
changes on those relations. It arose from the materialized view work and is currently only supporting 
materialized views for Postgres, Redshift, and BigQuery as well as dynamic tables for Snowflake. There are three main
classes in this package.

## RelationConfigBase
This is a very small class that only has a handful of methods. It's effectively a parser, but with two sources.
The objective is to provide a stopping point between dbt-specific config and database-specific config for two
primary reasons:

- apply validation rules in the parlance of the database
- articular what changes are monitored, and how those changes are applied in the database

At some point this could be theoretically be replaced by a more robust framework, like `mashumaro` or `pydantic`.

## RelationConfigChange
A `RelationConfigChange` can be thought of as being analogous to a web request on a `RelationConfigBase`.
You need to know what you're doing (`action`: 'create' = GET, 'drop' = DELETE, etc.)
and the information (`context`) needed to make the change.
In our scenarios, the context tends to be an instance of `RelationConfigBase` corresponding to the new state
or a single value if the change is simple.

## RelationConfigValidationMixin
This mixin provides optional validation mechanics that can be applied to either `RelationConfigBase` or
`RelationConfigChange` subclasses. A validation rule is a combination of a `validation_check`, something
that should evaluate to `True`, and an optional `validation_error`, an instance of `DbtRuntimeError`
that should be raised in the event the `validation_check` fails. While optional, it's recommended that
the `validation_error` be provided for clearer transparency to the end user.
