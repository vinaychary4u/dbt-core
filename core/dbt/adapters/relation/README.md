# Relation Models
This package serves as an initial abstraction for managing the inspection of existing relations and determining
changes on those relations. It arose from the materialized view work and is currently only supporting 
materialized views for Postgres, Redshift, and BigQuery as well as dynamic tables for Snowflake. There are three main
classes in this package.

## RelationFactory
This factory is the entrypoint that should be used to consistently create `Relation` objects. An instance of this
factory exists, and is configured, on `BaseAdapter` and its subclasses. Using this ensures that if a materialized view
relation is needed, one is always created using the same subclass of `Relation`. An adapter should take an instance
of this class in the `available` method `BaseAdapter.relation_factory()`. This factory also has some
useful shortcut methods for common operations in jinja:

- `make_from_node`
- `make_from_describe_relation_results`
- `make_ref`
- `make_backup_ref`
- `make_intermediate`
- `make_changeset`

In addition to being useful in its own right, this factory also gets passed to `Materialization` classes to
streamline jinja workflows. While the adapter maintainer could call `make_backup_ref` directly, it's more likely
that a process that takes a `Materialization` instance is doing that for them.
See `../materialization/README.md` for more information.

## Relation
This class holds the primary parsing methods required for marshalling data from a user config or a database metadata
query into a `Relation` subclass. `Relation` is a good class to subclass from for things like tables, views, etc.
The expectation is that a `Relation` is something that gets used with a `Materialization`. The intention is to
have some default implementations as built-ins for basic use/prototyping. So far there is only one.

### MaterializedViewRelation
This class is a basic materialized view that only has enough attribution to create and drop a materialized views.
There is no change management. However, as long as the required jinja templates are provided, this should just work.

## RelationComponent
This class is a boiled down version of `Relation` that still has some parsing functionality. `RelationComponent`
is a good class to subclass from for things like a Postgres index, a Redshift sortkey, a Snowflake target_lag, etc.
A `RelationComponent` should always be an attribute of a `Relation` or another `RelationComponent`. There are a
few built-ins that will likely be used in every `Relation`.

### Schema
This represents a database schema. It's very basic, and generally the only reason to subclass from it is to
apply some type of validation rule (e.g. the name can only be so long).

### Database
This represents a database. Like `Schema`, it's very basic, and generally the only reason to subclass from it is to
apply some type of validation rule (e.g. the name can only be so long).

## RelationRef
- `RelationRef`
- `SchemaRef`
- `DatabaseRef`

This collection of objects serves as a bare bones reference to a database object that can be used for small tasks,
e.g. `DROP`, `RENAME`. It really serves as a bridge between relation types that are build on this framework
and relation types that still reside on the existing framework. A materialized view will need to be able to
reference a table object that is sitting in the way and rename/drop it. Additionally, this provides a way to
reference an existing materialized view without querying the database to get all of the metadata. This step
is put off as late as possible to improve performance.

## RelationChange
This class holds the methods required for detecting and acting on changes on a `Relation`. All changes
should subclass from `RelationChange`. A `RelationChange` can be thought of as being analogous
to a web request on a `Relation`. You need to know what you're doing
(`action`: 'create' = GET, 'drop' = DELETE, etc.) and the information (`context`) needed to make the change.
In our scenarios, `context` tends to be either an instance of `RelationComponent` corresponding to the new state
or a single value if the change is simple. For example, creating an `index` would require the entire config;
whereas updating a setting like `autorefresh` for Redshift would require only the setting.

## RelationChangeset
This class is effectively a bin for collecting instances of `RelationChange`. It comes with a few helper
methods that facilitate rolling up concepts like `require_full_refresh` to the aggregate level.
