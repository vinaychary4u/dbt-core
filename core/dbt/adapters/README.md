# Adapters README

The Adapters module is responsible for defining database connection methods, caching information from databases, how relations are defined, and the two major connection types we have - base and sql.

# Directories

## `base`

Defines the base implementation Adapters can use to build out full functionality.

## `sql`

Defines a sql implementation for adapters that initially inherits the above base implementation and  comes with some premade methods and macros that can be overwritten as needed per adapter. (most common type of adapter.)

# Files

## `cache.py`

Cached information from the database.

## `factory.py`
Defines how we generate adapter objects

## `protocol.py`

Defines various interfaces for various adapter objects. Helps mypy correctly resolve methods.

## `reference_keys.py`

Configures naming scheme for cache elements to be universal.

## Validation

- `ValidationMixin`
- `ValidationRule`

These classes live in `validation.py`, outside of `relation` because they don't pertain specifically to `Relation`.
However, they are only currently used by `Relation`.
`ValidationMixin` provides optional validation mechanics that can be applied to either `Relation`, `RelationComponent`,
or `RelationChange` subclasses. To implement `ValidationMixin`, include it as a subclass in your `Relation`-like
object and add a method `validation_rules()` that returns a set of `ValidationRule` objects.
A `ValidationRule` is a combination of a `validation_check`, something that should always evaluate to `True`
in expected scenarios (i.e. a `False` is an invalid configuration), and an optional `validation_error`,
an instance of `DbtRuntimeError` that should be raised in the event the `validation_check` fails.
While optional, it's recommended that the `validation_error` be provided for clearer transparency to the end user
as the default does not know why the `validation_check` failed.
