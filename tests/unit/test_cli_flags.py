import pytest

import click
from multiprocessing import get_context
from typing import List

from dbt.cli.main import cli
from dbt.contracts.project import UserConfig
from dbt.cli.flags import Flags


class TestFlags:
    def make_dbt_context(self, context_name: str, args: List[str]) -> click.Context:
        ctx = cli.make_context(context_name, args)
        return ctx

    @pytest.fixture(scope="class")
    def run_context(self) -> click.Context:
        return self.make_dbt_context("run", ["run"])

    def test_which(self, run_context):
        flags = Flags(run_context)
        assert flags.WHICH == "run"

    def test_mp_context(self, run_context):
        flags = Flags(run_context)
        assert flags.MP_CONTEXT == get_context("spawn")

    @pytest.mark.parametrize("param", cli.params)
    def test_cli_group_flags_from_params(self, run_context, param):
        flags = Flags(run_context)
        assert hasattr(flags, param.name.upper())
        assert getattr(flags, param.name.upper()) == run_context.params[param.name.lower()]

    @pytest.mark.parametrize(
        "do_not_track,expected_anonymous_usage_stats",
        [
            ("1", False),
            ("t", False),
            ("true", False),
            ("y", False),
            ("yes", False),
            ("false", True),
            ("anything", True),
            ("2", True),
        ],
    )
    def test_anonymous_usage_state(
        self, monkeypatch, run_context, do_not_track, expected_anonymous_usage_stats
    ):
        monkeypatch.setenv("DO_NOT_TRACK", do_not_track)

        flags = Flags(run_context)
        assert flags.ANONYMOUS_USAGE_STATS == expected_anonymous_usage_stats

    def test_empty_user_config_uses_default(self, run_context):
        user_config = UserConfig()

        flags = Flags(run_context, user_config)
        assert flags.USE_COLORS == run_context.params["use_colors"]

    def test_none_user_config_uses_default(self, run_context):
        flags = Flags(run_context, None)
        assert flags.USE_COLORS == run_context.params["use_colors"]

    def test_prefer_user_config_to_default(self, run_context):
        user_config = UserConfig(use_colors=False)
        # ensure default value is not the same as user config
        assert run_context.params["use_colors"] is not user_config.use_colors

        flags = Flags(run_context, user_config)
        assert flags.USE_COLORS == user_config.use_colors

    def test_prefer_param_value_to_user_config(self):
        user_config = UserConfig(use_colors=False)
        context = self.make_dbt_context("run", ["--use-colors", "True", "run"])

        flags = Flags(context, user_config)
        assert flags.USE_COLORS

    def test_prefer_env_to_user_config(self, monkeypatch):
        user_config = UserConfig(use_colors=False)
        monkeypatch.setenv("DBT_USE_COLORS", "True")
        context = self.make_dbt_context("run", ["run"])

        flags = Flags(context, user_config)
        assert flags.USE_COLORS

    @pytest.mark.parametrize(
        "cli_colors,cli_colors_file,flag_colors,flag_colors_file",
        [
            (None, None, True, True),
            (True, None, True, True),
            (None, True, True, True),
            (False, None, False, False),
            (None, False, True, False),
            (True, True, True, True),
            (False, False, False, False),
            (True, False, True, False),
            (False, True, False, True),
        ],
    )
    def test_no_color_interaction(
        self, cli_colors, cli_colors_file, flag_colors, flag_colors_file
    ):
        cli_params = []

        if cli_colors is not None:
            cli_params.append("--use-colors" if cli_colors else "--no-use-colors")

        if cli_colors_file is not None:
            cli_params.append("--use-colors-file" if cli_colors_file else "--no-use-colors-file")

        cli_params.append("run")

        context = self.make_dbt_context("run", cli_params)

        flags = Flags(context, None)

        assert flags.USE_COLORS == flag_colors
        assert flags.USE_COLORS_FILE == flag_colors_file

    @pytest.mark.parametrize(
        "cli_log_level,cli_log_level_file,flag_log_level,flag_log_level_file",
        [
            (None, None, "info", "debug"),
            ("error", None, "error", "error"),  # explicit level overrides file level...
            ("info", None, "info", "info"),  # ...but file level doesn't change console level
            (
                "debug",
                "warn",
                "debug",
                "warn",
            ),  # still, two separate explicit levels are applied independently
        ],
    )
    def test_log_level_interaction(
        self, cli_log_level, cli_log_level_file, flag_log_level, flag_log_level_file
    ):
        cli_params = []

        if cli_log_level is not None:
            cli_params.append("--log-level")
            cli_params.append(cli_log_level)

        if cli_log_level_file is not None:
            cli_params.append("--log-level-file")
            cli_params.append(cli_log_level_file)

        cli_params.append("run")

        context = self.make_dbt_context("run", cli_params)

        flags = Flags(context, None)

        assert flags.LOG_LEVEL == flag_log_level
        assert flags.LOG_LEVEL_FILE == flag_log_level_file

    @pytest.mark.parametrize(
        "cli_log_format,cli_log_format_file,flag_log_format,flag_log_format_file",
        [
            (None, None, "default", "debug"),
            ("json", None, "json", "json"),  # explicit format overrides file format...
            (None, "json", "default", "json"),  # ...but file format doesn't change console format
            (
                "debug",
                "text",
                "debug",
                "text",
            ),  # still, two separate explicit formats are applied independently
        ],
    )
    def test_log_format_interaction(
        self, cli_log_format, cli_log_format_file, flag_log_format, flag_log_format_file
    ):
        cli_params = []

        if cli_log_format is not None:
            cli_params.append("--log-format")
            cli_params.append(cli_log_format)

        if cli_log_format_file is not None:
            cli_params.append("--log-format-file")
            cli_params.append(cli_log_format_file)

        cli_params.append("run")

        context = self.make_dbt_context("run", cli_params)

        flags = Flags(context, None)

        assert flags.LOG_FORMAT == flag_log_format
        assert flags.LOG_FORMAT_FILE == flag_log_format_file
