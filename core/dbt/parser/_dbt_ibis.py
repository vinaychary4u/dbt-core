"""
adapted from https://github.com/dbt-labs/dbt-core/pull/5982/files
"""
import ibis


def compile(code: str):
    # TODO: credentials from dbt
    import yaml

    # replace as needed
    PROFILE_PATH = "/Users/cody/.dbt/profiles.yml"
    PROFILE_NAME = "p-ibis"
    PROFILE_OUTPUT = "dev"

    # read in dbt profile
    with open(PROFILE_PATH, "r") as f:
        profiles = yaml.safe_load(f)
        profile = profiles[PROFILE_NAME]["outputs"][PROFILE_OUTPUT]

    # build connection parameters from profile
    conn_params = {
        "account": profile["account"],
        "user": profile["user"],
        "role": profile["role"],
        "warehouse": profile["warehouse"],
        "database": profile["database"],
        "schema": profile["schema"],
        "authenticator": profile["authenticator"],
    }

    s = ibis.connect(
        f"snowflake://{conn_params['user']}:_@{conn_params['account']}/{conn_params['database']}/{conn_params['schema']}?warehouse={conn_params['warehouse']}&role={conn_params['role']}&authenticator={conn_params['authenticator']}",
    )
    # TODO: replace above

    # the dirtiest code I've ever written?
    # run the ibis code and compile the `model` variable
    exec(code)
    compiled = str(eval("ibis.snowflake.compile(model)"))

    return compiled
