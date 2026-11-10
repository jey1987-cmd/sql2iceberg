"""Find the current metadata.json of Iceberg tables."""

from __future__ import annotations

import re
from pathlib import Path

from .base import IcebergTable

_VERSION = re.compile(r"^(?:v)?(\d+)(?:-[0-9a-f-]+)?\.(?:gz\.)?metadata\.json$")


def latest_local_metadata(warehouse: str | Path, namespace: str, table: str) -> Path:
    """Newest metadata file of a Hadoop-catalog table on a local or mounted path."""
    folder = Path(warehouse) / namespace / table / "metadata"
    best: tuple[int, Path] | None = None
    for f in folder.glob("*.metadata.json*"):
        m = _VERSION.match(f.name)
        if m and (best is None or int(m.group(1)) > best[0]):
            best = (int(m.group(1)), f)
    if best is None:
        raise FileNotFoundError(f"no metadata.json under {folder}")
    return best[1]


def metadata_from_spark(spark, catalog: str, namespace: str, table: str) -> str:
    """Current metadata location from Iceberg's metadata_log_entries (works for any catalog type)."""
    rows = spark.sql(
        f"SELECT file FROM {catalog}.{namespace}.{table}.metadata_log_entries ORDER BY timestamp DESC LIMIT 1"
    ).collect()
    if not rows:
        raise LookupError(f"{catalog}.{namespace}.{table} has no metadata log")
    return rows[0][0]


def tables_from_spark(spark, catalog: str, namespace: str) -> list[IcebergTable]:
    """Every table in a namespace with its current metadata location and format version."""
    out = []
    for row in spark.sql(f"SHOW TABLES IN {catalog}.{namespace}").collect():
        name = row["tableName"]
        loc = metadata_from_spark(spark, catalog, namespace, name)
        props = {r[0]: r[1] for r in spark.sql(f"SHOW TBLPROPERTIES {catalog}.{namespace}.{name}").collect()}
        out.append(IcebergTable(name, namespace, loc, int(props.get("format-version", 2))))
    return out


def tables_from_jdbc_catalog(sqlite_file: str | Path, namespace: str, catalog: str = "lake",
                             format_version: int = 2) -> list[IcebergTable]:
    """Tables and current metadata locations from an Iceberg JdbcCatalog stored in a SQLite file.

    Use this when the catalog is the SQLite file written by ``catalog_type="jdbc"``. ``format_version`` is not
    stored in the catalog table, so pass the version the tables were written with.
    """
    import sqlite3

    con = sqlite3.connect(str(sqlite_file))
    try:
        rows = con.execute(
            "SELECT table_name, metadata_location FROM iceberg_tables "
            "WHERE catalog_name = ? AND table_namespace = ? ORDER BY table_name", (catalog, namespace)).fetchall()
    finally:
        con.close()
    return [IcebergTable(n, namespace, loc, format_version) for n, loc in rows]
