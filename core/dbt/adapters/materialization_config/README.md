# Materialization Configs
This package serves as an initial abstraction for managing the inspection of existing relations and determining
changes on those relations. It arose from the materialized view work and is currently only supporting 
materialized views for Postgres, Redshift, and BigQuery as well as dynamic tables for Snowflake. There are three main
classes in this package.

## Base (Data)Classes
These are the core set of classes required to describe, validate, and monitor changes on database objects. All
other classes in `materialization_config` subclass from one of these classes.

### RelationConfig
This class holds the primary parsing methods required for marshalling data from a user config or a database metadata
query into a `RelationConfig` subclass. `RelationConfig` is a good class to subclass for smaller, atomic
database objects or objects that may be specific to a subset of adapters. For example, a Postgres index is modelled
from `RelationConfig` because not every database has an index and there is not much hierarchy to an index.

The objective of this parser is to provide a stopping point between dbt-specific config and database-specific config
for two primary reasons:

- apply validation rules in the parlance of the database
- articular what changes are monitored, and how those changes are applied in the database

At some point this could be theoretically be replaced by a more robust framework, like `mashumaro` or `pydantic`.

### RelationConfigChange
This class holds the methods required for detecting and acting on changes in a materialization. All changes
should subclass from `RelationConfigChange`. A `RelationConfigChange` can be thought of as being analogous
to a web request on a `RelationConfig`. You need to know what you're doing
(`action`: 'create' = GET, 'drop' = DELETE, etc.) and the information (`context`) needed to make the change.
In our scenarios, `context` tends to be either an instance of `RelationConfig` corresponding to the new state
or a single value if the change is simple. For example, creating an index would require the entire config;
whereas updating a setting like autorefresh for Redshift would require only the setting.

### RelationConfigChangeset
This class is effectively a bin for collecting instances of `RelationConfigChange`. It comes with a few helper
methods that facilitate rolling up concepts like `require_full_refresh` to the aggregate level.

### RelationConfigValidationMixin
This mixin provides optional validation mechanics that can be applied to either `RelationConfigBase` or
`RelationConfigChange` subclasses. A validation rule is a combination of a `validation_check`, something
that should evaluate to `True`, and an optional `validation_error`, an instance of `DbtRuntimeError`
that should be raised in the event the `validation_check` fails. While optional, it's recommended that
the `validation_error` be provided for clearer transparency to the end user.

## Basic Building Blocks (Data)Classes

### DatabaseConfig
This is the most basic version of `RelationConfig` that we can have. It adds a `name` and a `fully_qualified_path`
and nothing else. But we generally need a database when dbt runs. In particular, we need to reference a database
in other classes, like `SchemaConfig`.

### SchemaConfig
As with `DatabaseConfig`, this class is pretty basic. It's existence is the most important thing. While this
may not be needed for certain databases, it's prevalent enough that it's worth building it as an out-of-the-box
object.

### MaterializationConfig
This is the pearl in the sand. dbt generally interacts at the materialization level. As an adapter maintainer, you'll
need to subclass from most, if not all, objects in `materialization_config`; however you're likely doing so in order
to work with a `MaterializationConfig` subclass. This class has several helper methods that make it easier
to template sql in jinja.

### IncludePolicy
Identifies whether a component is included in a fully qualified path.

### QuotePolicy
Identifies whether a component is considered case-sensitive, or should be quoted, in a fully qualified path. This
config also contains the quote character for the database.

## Functions

### Policy
There are only a handful of functions here. In order to reduce duplication in docs, the reader is referred to the
docstrings on these functions for more detail:

- `conform_part`
- `render_part`
- `render`

### Materialization
This is a new class that serves as a service layer to expose `MaterializationConfig` functionality in the
jinja context. We're effectively tucking away the modelling of database objects in python and only exposing
class methods to serve as a basic API into `MaterializationConfig`.

- `make_backup` - create a backup materialization given an existing materialization
- `make_intermediate` - create an intermediate materialization given a target materialization
- `backup_name` - get the name that will be used in `make_backup`
- `intermediate_name` - get the name that will be used in `make_intermediate`
- `from_model_node` - build a `MaterializationConfig` from a `ModelNode` (`config.model` in jinja)
- `from_describe_relation_results` - build a `MaterializationConfig` from the database query results
- `materialization_configs` - a `dict` that registers a `MaterializationConfig` to a `RelationType`

### BaseRelation
There is a new method on `BaseRelation` that is meant to be used to interact with `MaterializationConfig`:

- `from_materialization_config` - build a `BaseRelation` from a `MaterializationConfig`; useful for using existing
functionality with the new structure

### BaseAdapter
The new `Materialization` object is registered on `BaseAdapter` and can be used in a similar fashion in jinja
as `BaseRelation` is used. There are also a few new helper methods:

- `get_cached_relation` - same as `get_relation`, but for `MaterializationConfig`
- `is_base_relation` - determines if the object is a `BaseRelation` instance
- `is_materialization_config` - determines if the object is a `MaterializationConfig`, usually paired with
`BaseRelation.from_materialization_config` to use existing functionality

It should be noted that `BaseAdapter.is_materialization_config` and `BaseRelation.from_materialization_config`
can be used to "merge" `BaseRelation` instances and `MaterializationConfig` instances into the same signature
in a jinja macro. This makes it so that you only need one macro, and can determine the pieces you need once you
get there. Generally speaking, you only need identifiers, schema names, types, etc. for templating anyway.
