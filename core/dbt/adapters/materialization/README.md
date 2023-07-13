# Materialization Models

## MaterializationFactory
Much like `RelationFactory` to `Relation`, this factory represents the way that `Materialization` instances should
be created. It guarantees that the same `RelationFactory`, and hence `Relation` subclasses, are always used. An
instance of this exists on `BaseAdapter`; however this will only need to be adjusted if a custom version of
`Materialization` is used. At the moment, this factory is sparce, with a single method for a single purpose:

- `make_from_node`

This method gets runs at the beginning of a materialization and that's about it. There is room for this to grow
as more complicated materializations arise.

## Materialization
A `Materialization` model is intended to represent a single materialization and all of the information required
to execute that materialization in a database. In many cases it can be confusing to differentiate between a
`Materialization` and a `Relation`. For example, a View materialization implements a View relation in the database.
However, the connection is not always one to one. As another example, both an incremental materialization and
a table materialization implement a table relation in the database. The separation between `Materialization`
and `Relation` is intended to separate the "what" from the "how". `Relation` corresponds to the "what"
and `Materialization` corresponds to the "how". That allows `Relation` to focus on what is needed to, for instance,
create a table in the database; on the other hand, `Materialization` might need to create several `Relation`
objects to accomplish its task.
