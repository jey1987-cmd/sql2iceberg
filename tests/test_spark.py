"""Executes converted SQL in local Spark on synthetic tables (needs sf2iceberg[pytest])."""
import datetime as dt

import pytest

pytestmark = pytest.mark.spark


@pytest.fixture(scope="module")
def tables(sf2i_spark):
    sf2i_spark.sql("DROP TABLE IF EXISTS orders")
    sf2i_spark.createDataFrame(
        [(1, dt.datetime(2026, 1, 5, 10), 10.0), (1, dt.datetime(2026, 2, 5, 10), 0.0), (2, dt.datetime(2026, 1, 20, 8), -5.0)],
        "id int, ts timestamp, amt double",
    ).write.mode("overwrite").saveAsTable("orders")


def test_qualify_runs_and_keeps_latest_row(sf2i_spark, tables, sf2i_convert):
    r = sf2i_convert(
        "SELECT id, ts, amt FROM orders QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1"
    )
    rows = sorted((x.id, x.amt) for x in sf2i_spark.sql(r.spark_sql).collect())
    assert rows == [(1, 0.0), (2, -5.0)]


def test_iff_runs(sf2i_spark, tables, sf2i_convert):
    r = sf2i_convert("SELECT id, IFF(amt > 0, 'pos', 'neg') AS s FROM orders")
    assert {x.s for x in sf2i_spark.sql(r.spark_sql).collect()} == {"pos", "neg"}


def test_dateadd_difference_is_real(sf2i_spark, tables, sf2i_convert):
    """Documents why DATEADD is flagged: Spark returns a DATE and drops the time of day."""
    r = sf2i_convert("SELECT DATEADD(day, 7, ts) AS d7 FROM orders")
    assert r.status == "review"
    value = sf2i_spark.sql(r.spark_sql).collect()[0].d7
    assert isinstance(value, dt.date) and not isinstance(value, dt.datetime)


@pytest.mark.live_snowflake
def test_live_comparison_placeholder():
    pytest.skip("live Snowflake comparison is not implemented yet (see docs/roadmap.md)")


def test_verify_flow_passes_for_qualify_case(sf2i_spark):
    from pathlib import Path

    from sf2iceberg.verify import verify_case

    case = Path(__file__).parent.parent / "examples" / "cases" / "qualify"
    ver = verify_case(case, spark=sf2i_spark)
    assert ver.outcome == "pass", ver.differences


def test_verify_flow_catches_dateadd_type_change(sf2i_spark):
    from pathlib import Path

    from sf2iceberg.verify import verify_case

    case = Path(__file__).parent.parent / "examples" / "cases" / "dateadd_timestamp"
    ver = verify_case(case, spark=sf2i_spark)
    assert ver.outcome == "fail" and ver.conversion_status == "review"
