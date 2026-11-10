"""Source connectors."""

from __future__ import annotations

from .base import ColumnInfo, Connector, TableInfo
from .others import MySQLConnector, OracleConnector, PostgresConnector, SQLServerConnector
from .snowflake import SnowflakeConnector

REGISTRY: dict[str, type[Connector]] = {
    c.name: c
    for c in (SnowflakeConnector, PostgresConnector, MySQLConnector, SQLServerConnector, OracleConnector)
}


def get_connector(name: str, **params: str) -> Connector:
    try:
        return REGISTRY[name.lower()](**params)
    except KeyError:
        raise ValueError(f"unknown platform {name!r}; choose from {sorted(REGISTRY)}") from None


__all__ = ["REGISTRY", "ColumnInfo", "Connector", "TableInfo", "get_connector"]
