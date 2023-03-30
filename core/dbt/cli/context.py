import click
from typing import Optional
import importlib

from dbt.cli.main import cli as dbt


def make_context(args, command=dbt) -> Optional[click.Context]:
    try:
        ctx = command.make_context(command.name, args)
    except click.exceptions.Exit:
        return None

    ctx.invoked_subcommand = ctx.protected_args[0] if ctx.protected_args else None
    ctx.obj = {}

    # Build MetricFlowClient if metricflow is installed (could move this to a click.option)
    ctx.obj["mf_client"] = None
    if importlib.util.find_spec("metricflow") is not None:
        from metricflow.api.metricflow_client import MetricFlowClient
        from metricflow.model.objects.user_configured_model import UserConfiguredModel
        from metricflow.sql_clients.sql_utils import make_sql_client

        # Fetch manifest and pull out what's needed to build UserConfiguredModel
        user_configured_model = UserConfiguredModel()

        # Parse profiles file to get DW connection details (url, pw, schema)
        sql_client = make_sql_client(url="", password="")

        ctx.obj["mf_client"] = MetricFlowClient(
            user_configured_model=user_configured_model,
            sql_client=sql_client,
            system_schema="",
        )

    return ctx
