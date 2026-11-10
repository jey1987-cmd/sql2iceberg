"""pytest plugin: fixtures and markers for validating converted SQL.

Installed through the `pytest11` entry point. Spark fixtures need `pip install 'sql2iceberg[pytest]'`.
Set SQL2ICE_ICEBERG=1 (and optionally ICEBERG_PKG) to add the Iceberg runtime and a Hadoop catalog.
"""

from __future__ import annotations

import os

import pytest

from .convert import convert_statement

DEFAULT_ICEBERG_PKG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2"


def pytest_addoption(parser):
    group = parser.getgroup("sql2iceberg")
    group.addoption("--live-snowflake", action="store_true", default=False,
                    help="run tests marked live_snowflake against a real Snowflake account (spends credits)")


def pytest_collection_modifyitems(config, items):
    if config.getoption("--live-snowflake"):
        return
    skip = pytest.mark.skip(reason="needs --live-snowflake (spends Snowflake credits)")
    for item in items:
        if "live_snowflake" in item.keywords:
            item.add_marker(skip)


@pytest.fixture(scope="session")
def sf2i_has_iceberg() -> bool:
    return os.environ.get("SQL2ICE_ICEBERG") == "1"


@pytest.fixture(scope="session")
def sf2i_spark(tmp_path_factory, sf2i_has_iceberg):
    pyspark_sql = pytest.importorskip("pyspark.sql")
    warehouse = str(tmp_path_factory.mktemp("warehouse"))
    builder = (
        pyspark_sql.SparkSession.builder.master("local[2]")
        .appName("sql2iceberg-tests")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.warehouse.dir", warehouse + "/spark")
    )
    if sf2i_has_iceberg:
        pkg = os.environ.get("ICEBERG_PKG", DEFAULT_ICEBERG_PKG)
        jar = os.environ.get("ICEBERG_JAR")  # local jar, for machines that cannot reach Maven Central
        builder = (
            builder.config("spark.jars", jar) if jar else builder.config("spark.jars.packages", pkg)
        )
        builder = (
            builder.config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
            .config("spark.sql.catalog.lake", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.lake.type", "hadoop")
            .config("spark.sql.catalog.lake.warehouse", warehouse)
            .config("spark.sql.defaultCatalog", "lake")
        )
    session = builder.getOrCreate()
    session.sparkContext.setLogLevel("ERROR")
    if sf2i_has_iceberg:
        session.sql("CREATE NAMESPACE IF NOT EXISTS lake.db")
        session.sql("USE lake.db")
    yield session
    session.stop()


@pytest.fixture()
def sf2i_convert():
    """Return a function that converts one Snowflake statement and returns a Result."""
    return convert_statement
