# Click powers dbt-core's CLI

## Context
 Previous to the implementation of this ADR, the code in `dbt-core` that handled CLI interactions was a combination of [`argparse`](https://docs.python.org/3/library/argparse.html) and a lot of custom code that altered its behavior by subclassing its components. However, as time progressed, it became clear that this approach was lacking in several regards: 
 * There needed to be a reasonable way to programmatically discover the various CLI commands and their available options. This made integrating other software with the API that the CLI represented especially challenging.
 * The arguments and argument groups were becoming more extensive and more complex. Because each grouping of arguments was created manually, this led to a lot of duplicated or unneeded code simply for the sake of organization.
 * `argparse` does not provide any plugin architecture or other methods to add functionality from third parties, leaving us to build any customizations entirely on our own. 

## Decision

 ### Research
  Research was done to discover an alternative as part of an overall API improvement effort. [Click](https://click.palletsprojects.com/) was selected to replace argparse for the following reasons:
  * Click provides a more robust ecosystem for building CLIs without being overly heavy-weight.
  * Click is probably the most widely adapted CLI framework for Python, which provides a significant mindshare in the dev community.
  * Click's composability allows for future code combining various commands into a single programmatic entry point with very little work.
  * Click provides a plugin ecosystem that allows us to take advantage of third-party contributions.

 ### Alternatives considered
  * No change
  * Typer
  * Docopt

 ### Proposal
  * We will create a Click application to replace the legacy argparse code.
  * We will build a robust series of Click parameters representing a contract for dbts CLI arguments.
  * We will ensure the Click app and its commands are importable and invokable by third parties.
  * We will document the Click app and its commands and provide them as a top-level programmatic interface for automating the usage of dbt.
 
## Status
 Accepted

## Consequences

 ### Negatives
  * Click is rather insistent on keeping the parameters from a parent command isolated from the child command. This didn't fit well with the multi-level-ness of the legacy interface and caused us to implement somewhat hacky logic to create a dummy click context and use that as a representation of the parent command. The primary example of this can be seen in `dbt.cli.flags`.
  * While they do provide a somewhat reasonable programmatic interface, invoking Click commands directly isn't ideal for the following reasons:
    * Exceptions are Click specific and don't provide a great UX.
    * Creating a Click context is somewhat cumbersome and not something an end-user should have to think about.
  `dbt.cli.main.dbtRunner` was created to address those issues.
  * IDEs don't properly introspect the decorator pattern used by Click to add parameters to commands.

 ### Positives
  * dbts commands are programmatically discoverable.
  * dbts commands can be imported and invoked by third parties.
  * Devs working on dbt can leverage Click [plugins/contribs](https://github.com/click-contrib).
  * dbts CLI parameters are verified more completely.
  * dbts CLI code is much easier to read and reason about.
