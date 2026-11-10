"""Connect to Snowflake using environment variables (or a git-ignored .env) and print who/where we are.

Runs only free metadata queries; it does not start a warehouse. Usage: python scripts/snowflake_connect.py
"""
import os
import sys

import snowflake.connector
from dotenv import load_dotenv


def connect(**overrides):
    """Open a connection from SNOWFLAKE_* variables. Warehouse/database/schema are optional (may not exist yet)."""
    load_dotenv()
    params = {
        "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
        "user": os.environ.get("SNOWFLAKE_USER"),
        "role": os.environ.get("SNOWFLAKE_ROLE") or None,
    }
    if os.environ.get("SNOWFLAKE_AUTHENTICATOR"):
        params["authenticator"] = os.environ["SNOWFLAKE_AUTHENTICATOR"]
    elif os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
        params["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    else:
        params["password"] = os.environ.get("SNOWFLAKE_PASSWORD")
    for key in ("warehouse", "database", "schema"):
        if overrides.get(key) is not False and os.environ.get(f"SNOWFLAKE_{key.upper()}"):
            params[key] = os.environ[f"SNOWFLAKE_{key.upper()}"]
    missing = [k for k in ("account", "user") if not params.get(k)]
    if missing:
        sys.exit(f"Missing {missing}: fill in .env (see .env.example) or set SNOWFLAKE_* variables.")
    return snowflake.connector.connect(**params)


if __name__ == "__main__":
    # Connect without warehouse/database/schema so this works before the setup SQL has been run.
    with connect(warehouse=False, database=False, schema=False) as conn:
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_ACCOUNT(), CURRENT_REGION(), CURRENT_ROLE(), CURRENT_VERSION()")
        print(dict(zip(("account", "region", "role", "version"), cur.fetchone())))
