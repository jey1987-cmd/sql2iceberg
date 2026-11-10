from __future__ import annotations

import os

from .base import Connector


class SnowflakeConnector(Connector):
    name = "snowflake"
    sqlglot_dialect = "snowflake"
    extra = "snowflake"
    tested = True
    required_env = ("SNOWFLAKE_ACCOUNT", "SNOWFLAKE_USER")
    jdbc_driver = "net.snowflake.client.jdbc.SnowflakeDriver"
    jdbc_user_env = "SNOWFLAKE_USER"
    jdbc_password_env = "SNOWFLAKE_PASSWORD"
    jdbc_package = "net.snowflake:snowflake-jdbc:3.19.0"
    jdbc_key_env = "SNOWFLAKE_PRIVATE_KEY_PATH"
    jdbc_key_option = "private_key_file"

    def connect(self):
        sf = self._require_driver("snowflake.connector")
        p = {"account": os.environ["SNOWFLAKE_ACCOUNT"], "user": os.environ["SNOWFLAKE_USER"]}
        if os.environ.get("SNOWFLAKE_ROLE"):
            p["role"] = os.environ["SNOWFLAKE_ROLE"]
        if os.environ.get("SNOWFLAKE_AUTHENTICATOR"):
            p["authenticator"] = os.environ["SNOWFLAKE_AUTHENTICATOR"]
        elif os.environ.get("SNOWFLAKE_PRIVATE_KEY_PATH"):
            p["private_key_file"] = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
        else:
            p["password"] = os.environ.get("SNOWFLAKE_PASSWORD", "")
        for key in ("warehouse", "database", "schema"):
            value = self.params.get(key) or os.environ.get(f"SNOWFLAKE_{key.upper()}")
            if value:
                p[key] = value
        self._conn = sf.connect(**p)
        return self._conn

    def jdbc_url(self) -> str:
        return f"jdbc:snowflake://{os.environ['SNOWFLAKE_ACCOUNT']}.snowflakecomputing.com/"

    def _tables_sql(self, schema):
        sql = ("SELECT table_schema, table_name, column_name, data_type, is_nullable "
               "FROM information_schema.columns WHERE table_schema <> 'INFORMATION_SCHEMA'")
        if schema:
            return sql + " AND table_schema = %s ORDER BY 1, 2, ordinal_position", (schema.upper(),)
        return sql + " ORDER BY 1, 2, ordinal_position", ()
