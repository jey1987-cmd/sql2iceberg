"""Connector interface: every source platform implements the same small surface.

Credentials are never read from config files. Each connector reads them from the environment
(or the platform's own profile/SSO mechanism) so the user sets up authentication before running.
"""

from __future__ import annotations

import os
from abc import ABC, abstractmethod
from dataclasses import dataclass, field


@dataclass
class ColumnInfo:
    name: str
    data_type: str
    nullable: bool = True


@dataclass
class TableInfo:
    schema: str
    name: str
    columns: list[ColumnInfo] = field(default_factory=list)

    @property
    def qualified(self) -> str:
        return f"{self.schema}.{self.name}"


class Connector(ABC):
    """A live connection to a source system."""

    #: registry key
    name: str = ""
    #: SQLGlot dialect used to parse this platform's SQL
    sqlglot_dialect: str = ""
    #: pip extra that installs the driver, e.g. ``pip install sql2iceberg[snowflake]``
    extra: str = ""
    #: only Snowflake is verified against a live system
    tested: bool = False
    #: env vars that must be set; checked for a clear error before connecting
    required_env: tuple[str, ...] = ()
    #: JDBC driver class so Spark can read the source in parallel
    jdbc_driver: str = ""
    #: env vars holding the JDBC login, and the Maven coordinate of the driver (review the version)
    jdbc_user_env: str = ""
    jdbc_password_env: str = ""
    jdbc_package: str = ""
    #: optional key-pair login: env var with the private-key file path, and the JDBC property that takes it
    jdbc_key_env: str = ""
    jdbc_key_option: str = ""

    def __init__(self, **params: str) -> None:
        self.params = params
        self._conn = None

    @classmethod
    def missing_env(cls, env: dict[str, str] | None = None) -> list[str]:
        env = os.environ if env is None else env
        return [k for k in cls.required_env if not env.get(k)]

    @abstractmethod
    def connect(self):
        """Open the DB-API connection, store it on ``self._conn`` and return it."""

    @abstractmethod
    def jdbc_url(self) -> str:
        """JDBC URL for Spark reads (never contains credentials)."""

    @abstractmethod
    def _tables_sql(self, schema: str | None) -> tuple[str, tuple]:
        """SQL returning (schema, table, column, type, nullable) rows, plus its parameters."""

    def _count_sql(self, table: str) -> str:
        return f"SELECT COUNT(*) FROM {table}"

    def _require_driver(self, module: str):
        import importlib

        try:
            return importlib.import_module(module)
        except ImportError as exc:
            raise ImportError(
                f"{self.name} support needs its driver: pip install 'sql2iceberg[{self.extra}]'"
            ) from exc

    def _ensure(self):
        if self._conn is None:
            missing = self.missing_env()
            if missing:
                raise RuntimeError(f"Set these environment variables first: {', '.join(missing)}")
            self.connect()
        return self._conn

    def query(self, sql: str, params: tuple = ()) -> list[tuple]:
        cur = self._ensure().cursor()
        try:
            cur.execute(sql, params) if params else cur.execute(sql)
            return list(cur.fetchall())
        finally:
            cur.close()

    def list_tables(self, schema: str | None = None) -> list[TableInfo]:
        sql, params = self._tables_sql(schema)
        tables: dict[tuple[str, str], TableInfo] = {}
        for sch, tbl, col, dtype, nullable in self.query(sql, params):
            info = tables.setdefault((sch, tbl), TableInfo(sch, tbl))
            info.columns.append(ColumnInfo(col, dtype, str(nullable).upper() in ("YES", "Y", "1", "TRUE")))
        return list(tables.values())

    def row_count(self, table: str) -> int:
        return int(self.query(self._count_sql(table))[0][0])

    def close(self) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    def __enter__(self):
        self._ensure()
        return self

    def __exit__(self, *exc) -> None:
        self.close()
