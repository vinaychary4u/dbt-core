# Relation Macro Templates

## Composite Macro Templates

Macros in `/composite/` are composites of atomic macros (e.g. `create_template`, `drop_template`,
`rename_template`, etc. In other words, they don't dispatch directly to a relation_type-specific macro, nor do
they contain sql of their own. They are effectively logic flow to perform transactions that are a combination of
atomic statements. This is done to minimize the amount of sql that is written in jinja and remove redundancy.

It's unlikely that these macros will need to be overridden; instead, the adapter maintainer is encouraged to
override the atomic components (e.g. `create_template`, `drop_template`, `rename_template`, etc.). Not only will
this minimize the amount of marginal maintenance within an adapter, it will also unlock all of the functionality
in these composite macros as a result.

## Atomic Macro Templates

Macros in `/atomic/` represent atomic actions on the database. They aren't necessarily transactions, nor are they
single statements; they are somewhere in between. They should be thought of as atomic at the `Relation` level in
the sense that you can't break down the action any further without losing a part of the relation, or a part of the
action on the relation. For example, the `create` action for a Postgres materialized view is actually a CREATE
statement followed by a series of CREATE INDEX statements. We wouldn't want to create the materialized view
without also creating all of its components, so that's one atomic action. Many actions are straight-forward,
(e.g. `drop` and `rename`) while others are less so (e.g. `alter` and `create`). Another way to think about it
is that all of these actions focus on exactly one relation, hence have a single `relation_type`. Even
`alter_template`, which takes in two `Relation` objects, is really just saying "I want `existing_relation` to
look like `"this"`"; `"this"` just happens to be another `Relation` object that contains all of the same
attributes, some with different values.

While these actions are atomic, the macros in this directory represent `relation_type`-agnostic actions.
For example, if you want to create a view, execute `create_template(my_view_relation)`. Since `my_view_relation`
has a `relation_type` of `materialized_view`, `create_template` will know to dispatch the call to
`create_materialized_view_template`. If the maintainer looks at any macro in this directory, they will see that
the macro merely dispatches to the `relation_type`-specific version. Hence, there are only two reasons to override
this macro:

1. The adapter supports more/less `relation-type`s than the default
2. The action can be consolidated into the same statement regardless of `relation_type`

## Atomic Macro Templates by Relation_Type

The most likely place that the adapter maintainer should look when overriding macros with adapter-specific
logic is in the relation-specific directories. Those are the directories in `/relations/` that have names
corresponding to `relation_type`s (e.g. `/materialized_view/`, `/view/`, etc.). At the `dbt-core` level,
macros in these directories will default to a version that throws an exception until implemented, much like
an abstract method in python. The intention is to make no assumptions about how databases work to avoid building
dependencies between database platforms within dbt. At the `dbt-<adapter>` level, each of these files should
correspond to a specific statement (give or take) from that database platform's documentation. For example,
the macro `postgres__create_materialized_view_template` aligns with the documentation found here:
https://www.postgresql.org/docs/current/sql-creatematerializedview.html. Ideally, once this macro is created,
there is not much reason to perform maintenance on it unless the database platform deploys new functionality
and dbt (or the adapter) has chosen to support that functionality.
