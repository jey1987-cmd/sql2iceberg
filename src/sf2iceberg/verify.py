"""Offline verification of one converted statement.

A case folder holds case.json, seed CSV files and an expected-results CSV. The expected CSV is what
the original Snowflake query returned on the same seed data (export it from your own Snowflake
account). The converted Spark SQL is run on the seed data and compared with it.
"""

from __future__ import annotations

import json
import math
import os
from dataclasses import dataclass, field
from pathlib import Path

from .convert import UNPARSED, UNSUPPORTED, convert_statement

PASS, FAIL, BLOCKED = "pass", "fail", "blocked"


@dataclass
class Verification:
    case: str
    outcome: str                       # pass | fail | blocked
    conversion_status: str
    spark_sql: str | None = None
    differences: list[str] = field(default_factory=list)
    findings: list[dict] = field(default_factory=list)

    def to_dict(self) -> dict:
        return self.__dict__.copy()


def make_spark(iceberg: bool = False, warehouse: str | None = None):
    from pyspark.sql import SparkSession

    builder = (
        SparkSession.builder.master("local[2]")
        .appName("sf2iceberg-verify")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
    )
    if iceberg:
        pkg = os.environ.get(
            "ICEBERG_PKG", "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"
        )
        builder = (
            builder.config("spark.jars.packages", pkg)
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", warehouse or "/tmp/sf2iceberg-warehouse")
            .config("spark.sql.defaultCatalog", "lake")
        )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    if iceberg:
        spark.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
        spark.sql("USE lake.db")
    return spark


def _same(a, b, tol: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    if isinstance(a, float) or isinstance(b, float):
        try:
            return math.isclose(float(a), float(b), rel_tol=tol, abs_tol=tol)
        except (TypeError, ValueError):
            return False
    return type(a) is type(b) and a == b


def _rows(df) -> list[tuple]:
    return [tuple(r) for r in df.collect()]


def _sort_key(row: tuple):
    return tuple((v is None, str(type(v)), str(v)) for v in row)


def verify_case(case_dir: str | Path, spark=None, iceberg: bool = False) -> Verification:
    case_dir = Path(case_dir)
    spec = json.loads((case_dir / "case.json").read_text(encoding="utf-8"))
    name = spec.get("name", case_dir.name)
    result = convert_statement(spec["sql"])
    ver = Verification(
        case=name,
        outcome=BLOCKED,
        conversion_status=result.status,
        spark_sql=result.spark_sql,
        findings=[f.__dict__ for f in result.findings],
    )
    if result.status in (UNSUPPORTED, UNPARSED):
        ver.differences.append("not executed: conversion is " + result.status)
        return ver

    own_session = spark is None
    spark = spark or make_spark(iceberg)
    try:
        for table, t in spec["tables"].items():
            df = spark.read.csv(str(case_dir / t["csv"]), header=True, schema=t["schema"])
            if iceberg:
                spark.sql(f"DROP TABLE IF EXISTS {table}")
                df.writeTo(table).using("iceberg").create()
            else:
                df.createOrReplaceTempView(table)  # no files left behind between runs
        try:
            got = _rows(spark.sql(result.spark_sql))
        except Exception as exc:  # noqa: BLE001 - report any Spark failure as a failed verification
            ver.outcome = FAIL
            ver.differences.append(f"Spark failed to run the converted SQL: {str(exc).splitlines()[0][:200]}")
            return ver
        expected = _rows(
            spark.read.csv(str(case_dir / spec["expected"]), header=True, schema=spec["expected_schema"])
        )
    finally:
        if own_session:
            spark.stop()

    if not spec.get("ordered", False):
        got, expected = sorted(got, key=_sort_key), sorted(expected, key=_sort_key)
    tol = float(spec.get("float_tolerance", 1e-9))
    if len(got) != len(expected):
        ver.differences.append(f"row count: Spark {len(got)} vs expected {len(expected)}")
    for i, (g, e) in enumerate(zip(got, expected)):
        if len(g) != len(e) or not all(_same(x, y, tol) for x, y in zip(g, e)):
            ver.differences.append(f"row {i}: Spark {g!r} vs expected {e!r}")
            if len(ver.differences) >= 6:
                break
    ver.outcome = PASS if not ver.differences else FAIL
    return ver
