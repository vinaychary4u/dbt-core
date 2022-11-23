"""
adapted from https://github.com/dbt-labs/dbt-core/pull/5982/files
"""
import ibis


def compile(code: str, context):

    conn_params = {
        "account": context["target"]["account"],
        "user": context["target"]["user"],
        "role": context["target"]["role"],
        "warehouse": context["target"]["warehouse"],
        "database": context["target"]["database"],
        "schema": context["target"]["schema"],
        "authenticator": "externalbrowser",
    }
    
    s = ibis.connect(
        f"snowflake://{conn_params['user']}:_@{conn_params['account']}/{conn_params['database']}/{conn_params['schema']}?warehouse={conn_params['warehouse']}&role={conn_params['role']}&authenticator={conn_params['authenticator']}",
    )

    # the dirtiest code I've ever written?
    # run the ibis code and compile the `model` variable
    exec(code)
    compiled = str(eval(f"ibis.{context['target']['type']}.compile(model)"))

    return compiled
