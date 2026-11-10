"""Postgres, MySQL, SQL Server and Oracle connectors.

EXPERIMENTAL: written to the same interface as Snowflake and covered by mock-based unit tests only.
They have not been run against live servers.
"""

from __future__ import annotations

import os

from .base import Connector

_INFO_SCHEMA = (
    "SELECT table_schema, table_name, column_name, data_type, is_nullable "
    "FROM information_schema.columns WHERE table_schema NOT IN ({excl})"
)


def _info_schema_query(excl: str, schema: str | None, marker: str = "%s"):
    sql = _INFO_SCHEMA.format(excl=excl)
    if schema:
        return sql + f" AND table_schema = {marker} ORDER BY 1, 2, ordinal_position", (schema,)
    return sql + " ORDER BY 1, 2, ordinal_position", ()


class PostgresConnector(Connector):
    name = "postgres"
    sqlglot_dialect = "postgres"
    extra = "postgres"
    required_env = ("PGHOST", "PGUSER", "PGDATABASE")
    jdbc_driver = "org.postgresql.Driver"
    jdbc_user_env = "PGUSER"
    jdbc_password_env = "PGPASSWORD"
    jdbc_package = "org.postgresql:postgresql:42.7.4"

    def connect(self):
        pg = self._require_driver("psycopg2")
        self._conn = pg.connect(
            host=os.environ["PGHOST"], port=os.environ.get("PGPORT", "5432"),
            user=os.environ["PGUSER"], password=os.environ.get("PGPASSWORD"),
            dbname=os.environ["PGDATABASE"],
        )
        return self._conn

    def jdbc_url(self) -> str:
        e = os.environ
        return f"jdbc:postgresql://{e['PGHOST']}:{e.get('PGPORT', '5432')}/{e['PGDATABASE']}"

    def _tables_sql(self, schema):
        return _info_schema_query("'pg_catalog','information_schema'", schema)


class MySQLConnector(Connector):
    name = "mysql"
    sqlglot_dialect = "mysql"
    extra = "mysql"
    required_env = ("MYSQL_HOST", "MYSQL_USER", "MYSQL_DATABASE")
    jdbc_driver = "com.mysql.cj.jdbc.Driver"
    jdbc_user_env = "MYSQL_USER"
    jdbc_password_env = "MYSQL_PASSWORD"
    jdbc_package = "com.mysql:mysql-connector-j:8.4.0"

    def connect(self):
        my = self._require_driver("pymysql")
        self._conn = my.connect(
            host=os.environ["MYSQL_HOST"], port=int(os.environ.get("MYSQL_PORT", "3306")),
            user=os.environ["MYSQL_USER"], password=os.environ.get("MYSQL_PASSWORD", ""),
            database=os.environ["MYSQL_DATABASE"],
        )
        return self._conn

    def jdbc_url(self) -> str:
        e = os.environ
        return f"jdbc:mysql://{e['MYSQL_HOST']}:{e.get('MYSQL_PORT', '3306')}/{e['MYSQL_DATABASE']}"

    def _tables_sql(self, schema):
        return _info_schema_query("'mysql','information_schema','performance_schema','sys'", schema)


class SQLServerConnector(Connector):
    name = "mssql"
    sqlglot_dialect = "tsql"
    extra = "mssql"
    required_env = ("MSSQL_SERVER", "MSSQL_DATABASE")
    jdbc_driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    jdbc_user_env = "MSSQL_USER"
    jdbc_password_env = "MSSQL_PASSWORD"
    jdbc_package = "com.microsoft.sqlserver:mssql-jdbc:12.8.1.jre11"

    def connect(self):
        py = self._require_driver("pyodbc")
        e = os.environ
        auth = ("Trusted_Connection=yes" if not e.get("MSSQL_USER")
                else f"UID={e['MSSQL_USER']};PWD={e.get('MSSQL_PASSWORD', '')}")
        driver = e.get("MSSQL_ODBC_DRIVER", "ODBC Driver 18 for SQL Server")
        self._conn = py.connect(
            f"DRIVER={{{driver}}};SERVER={e['MSSQL_SERVER']};DATABASE={e['MSSQL_DATABASE']};{auth}"
        )
        return self._conn

    def jdbc_url(self) -> str:
        e = os.environ
        return f"jdbc:sqlserver://{e['MSSQL_SERVER']};databaseName={e['MSSQL_DATABASE']}"

    def _tables_sql(self, schema):
        return _info_schema_query("'sys','INFORMATION_SCHEMA'", schema, marker="?")


class OracleConnector(Connector):
    name = "oracle"
    sqlglot_dialect = "oracle"
    extra = "oracle"
    required_env = ("ORACLE_DSN", "ORACLE_USER")
    jdbc_driver = "oracle.jdbc.OracleDriver"
    jdbc_user_env = "ORACLE_USER"
    jdbc_password_env = "ORACLE_PASSWORD"
    jdbc_package = "com.oracle.database.jdbc:ojdbc11:23.5.0.24.07"

    def connect(self):
        ora = self._require_driver("oracledb")
        self._conn = ora.connect(user=os.environ["ORACLE_USER"],
                                 password=os.environ.get("ORACLE_PASSWORD"), dsn=os.environ["ORACLE_DSN"])
        return self._conn

    def jdbc_url(self) -> str:
        return f"jdbc:oracle:thin:@{os.environ['ORACLE_DSN']}"

    def _tables_sql(self, schema):
        sql = ("SELECT owner, table_name, column_name, data_type, nullable FROM all_tab_columns "
               "WHERE owner NOT IN ('SYS','SYSTEM')")
        if schema:
            return sql + " AND owner = :1 ORDER BY 1, 2, column_id", (schema.upper(),)
        return sql + " ORDER BY 1, 2, column_id", ()
