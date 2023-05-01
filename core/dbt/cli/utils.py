from dbt.cli.flags import Flags
from dbt.config import RuntimeConfig
from dbt.config.runtime import Profile, Project, load_project, load_profile


def get_profile(flags: Flags) -> Profile:
    # TODO: Generalize safe access to flags.THREADS:
    # https://github.com/dbt-labs/dbt-core/issues/6259
    threads = getattr(flags, "THREADS", None)
    return load_profile(flags.PROJECT_DIR, flags.VARS, flags.PROFILE, flags.TARGET, threads)


def get_project(flags: Flags, profile: Profile) -> Project:
    return load_project(
        flags.PROJECT_DIR,
        flags.VERSION_CHECK,
        profile,
        flags.VARS,
    )


def get_runtime_config(flags: Flags) -> RuntimeConfig:
    profile = get_profile(flags)
    project = get_project(flags, profile)
    return RuntimeConfig.from_parts(
        args=flags,
        profile=profile,
        project=project,
    )
