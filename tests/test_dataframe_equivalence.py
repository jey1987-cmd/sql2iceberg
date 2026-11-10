"""The DataFrame steps the generator builds must return exactly what the same query returns as plain Spark SQL.

Each Snowflake query below is converted, planned in ``mixed`` mode (DataFrame API wherever possible) and executed in a local
Spark session; the rows are compared with the converted SQL run directly. Needs ``sql2iceberg[pytest]`` (PySpark and Java 17).
"""
import datetime as dt

import pytest
import sqlglot

from sql2iceberg.convert import convert_statement
from sql2iceberg.dataframe_gen import HELPERS, plan_statement
from sql2iceberg.pyspark_gen import MigrationConfig, _qualify

pytestmark = pytest.mark.spark

CASES = {
    "cte_group_having_join": """
        CREATE TABLE t AS
        WITH big AS (SELECT customer_id, SUM(amount) AS spend, COUNT(*) AS n FROM orders
                     WHERE status <> 'cancelled' GROUP BY customer_id HAVING SUM(amount) > 15)
        SELECT c.name, b.spend, b.n FROM customers c JOIN big b ON b.customer_id = c.customer_id""",
    "window_over_aggregate_falls_back_to_sql": """
        CREATE TABLE t AS
        WITH m AS (SELECT status, SUM(amount) AS s, SUM(SUM(amount)) OVER (ORDER BY status) AS running
                   FROM orders GROUP BY status)
        SELECT status, s, running FROM m WHERE s IS NOT NULL""",
    "union_all_and_union": """
        CREATE TABLE t AS
        WITH a AS (SELECT customer_id AS k FROM orders), b AS (SELECT customer_id AS k FROM customers)
        SELECT k FROM a UNION ALL SELECT k FROM b UNION SELECT 99""",
    "pivot": """
        CREATE TABLE t AS
        SELECT country, o_open, o_closed
        FROM (SELECT c.country, o.status, o.amount FROM orders o JOIN customers c ON c.customer_id = o.customer_id)
        PIVOT (SUM(amount) FOR status IN ('open', 'closed')) AS p (country, o_open, o_closed)""",
    "left_join_order_limit": """
        CREATE TABLE t AS
        WITH x AS (SELECT c.customer_id, c.name, o.amount FROM customers c LEFT JOIN orders o ON o.customer_id = c.customer_id)
        SELECT name, amount FROM x ORDER BY amount DESC NULLS LAST, name LIMIT 4""",
    "from_subquery_with_alias_columns": """
        CREATE TABLE t AS
        SELECT s.cid, s.total FROM (SELECT customer_id, SUM(amount) AS t FROM orders GROUP BY customer_id) AS s (cid, total)
        WHERE s.total > 5""",
    "in_subquery_and_not_exists": """
        CREATE TABLE t AS
        WITH vip AS (SELECT customer_id FROM orders GROUP BY customer_id HAVING SUM(amount) > 20)
        SELECT c.customer_id, c.name FROM customers c
        WHERE c.customer_id IN (SELECT customer_id FROM vip)
          AND NOT EXISTS (SELECT 1 FROM orders o WHERE o.customer_id = c.customer_id AND o.status = 'cancelled')""",
    "flatten_value_and_index": """
        CREATE TABLE t AS
        WITH base AS (SELECT customer_id, tags FROM customers WHERE tags IS NOT NULL)
        SELECT f.value::string AS tag, f.index AS pos, COUNT(*) AS n
        FROM base b, LATERAL FLATTEN(input => b.tags) f GROUP BY f.value::string, f.index""",
    "listagg_and_array_agg_within_group": """
        CREATE TABLE t AS
        WITH o AS (SELECT * FROM orders WHERE amount IS NOT NULL)
        SELECT customer_id, LISTAGG(DISTINCT status, ',') WITHIN GROUP (ORDER BY status) AS statuses,
               ARRAY_TO_STRING(ARRAY_AGG(order_id) WITHIN GROUP (ORDER BY order_id DESC), '|') AS ids
        FROM o GROUP BY customer_id""",
    "global_aggregate": """
        CREATE TABLE t AS
        WITH o AS (SELECT amount FROM orders WHERE status = 'closed')
        SELECT COUNT(*) AS n, SUM(amount) AS total, MAX(amount) AS biggest FROM o""",
    "group_by_ordinals_and_expression_key": """
        CREATE TABLE t AS
        WITH o AS (SELECT * FROM orders)
        SELECT DATE_TRUNC('month', order_ts) AS m, status, SUM(amount) AS s FROM o GROUP BY 1, 2""",
    "distinct_and_case": """
        CREATE TABLE t AS
        WITH o AS (SELECT customer_id, IFF(amount > 10, 'big', 'small') AS band FROM orders)
        SELECT DISTINCT customer_id, band FROM o""",
    "percentile_and_datediff": """
        CREATE TABLE t AS
        WITH o AS (SELECT * FROM orders)
        SELECT status, PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY amount) AS med,
               MAX(DATEDIFF('day', DATE '2026-01-01', order_ts)) AS days
        FROM o GROUP BY status""",
}


@pytest.fixture(scope="module")
def data(sf2i_spark):
    sf2i_spark.createDataFrame(
        [(1, 1, "open", 10.0, dt.datetime(2026, 1, 5, 23, 30)), (2, 1, "closed", 7.5, dt.datetime(2026, 1, 6, 0, 10)),
         (3, 2, "closed", 30.0, dt.datetime(2026, 2, 1, 9, 0)), (4, 2, "cancelled", 4.0, dt.datetime(2026, 2, 3, 9, 0)),
         (5, 3, "open", None, dt.datetime(2026, 2, 3, 12, 0)), (6, 3, "open", 3.0, dt.datetime(2026, 3, 1, 1, 0))],
        "order_id int, customer_id int, status string, amount double, order_ts timestamp",
    ).write.mode("overwrite").saveAsTable("orders")
    sf2i_spark.createDataFrame(
        [(1, "Ann", "US", '["a","b"]'), (2, "Bo", "DE", '["a"]'), (3, "Cy", "US", None), (4, "Di", "DE", '["c","a"]')],
        "customer_id int, name string, country string, tags string",
    ).write.mode("overwrite").saveAsTable("customers")


def _rows(df):
    out = []
    for r in df.collect():
        out.append(tuple(round(v, 6) if isinstance(v, float) else v for v in r))
    return sorted(out, key=str)


@pytest.mark.parametrize("name", sorted(CASES))
def test_mixed_plan_returns_the_same_rows_as_spark_sql(name, sf2i_spark, data):
    import pyspark.sql.functions as F

    result = convert_statement(CASES[name])
    assert result.spark_sql, result.findings
    cfg = MigrationConfig(catalog="spark_catalog", namespace="default", jdbc_url="jdbc:x")
    tree = _qualify(sqlglot.parse_one(result.spark_sql, read="spark"), cfg)
    plan = plan_statement(tree, "", mode="mixed")
    assert plan.strategy == "mixed"
    code = "\n".join(line for line in plan.lines if not line.startswith("result.writeTo"))
    env = {"spark": sf2i_spark, "F": F, "FORMAT_VERSION": 2}
    for helper in plan.helpers:
        exec(HELPERS[helper], env)            # noqa: S102  generated helper source
    exec(code, env)                           # noqa: S102  generated code under test
    expected = sf2i_spark.sql(tree.expression.sql(dialect="spark"))
    assert _rows(env["result"]) == _rows(expected)
    if name.endswith("falls_back_to_sql"):
        assert plan.sql_steps >= 1
    else:
        assert plan.df_steps >= 1


def test_validate_from_generated_config_catches_a_wrong_table(sf2i_spark, data, monkeypatch):
    """The generated validate.json shape: original SELECT on the 'source' vs the Spark table, columns taken from the table."""
    from sql2iceberg import validate as v

    for name, total in (("good_tbl", "SUM(amount)"), ("bad_tbl", "SUM(amount) + 1")):
        sf2i_spark.sql(f"DROP TABLE IF EXISTS {name}")
        sf2i_spark.sql(f"CREATE TABLE {name} AS SELECT customer_id, {total} AS total FROM orders GROUP BY customer_id")
    reference = v.Target("source", lambda sql: [tuple(r) for r in sf2i_spark.sql(sql).collect()], {}, "DOUBLE")
    monkeypatch.setattr(v, "_target_from_config", lambda cfg: v.Target("source", reference.run, cfg["tables"], "DOUBLE"))
    select = "SELECT customer_id, SUM(amount) AS total FROM orders GROUP BY customer_id"
    cfg = {"tables": "auto", "reference": {"kind": "snowflake", "name": "source", "queries": {"good": [select], "bad": [select]}},
           "targets": [{"kind": "spark", "name": "iceberg", "tables": {"good": "good_tbl", "bad": "bad_tbl"}}]}
    rep = v.report_from_config(cfg, spark=sf2i_spark)
    assert not rep.ok
    failed = {r.table for r in rep.results if not r.ok}
    assert failed == {"bad"} and any(r.check == "sum:total" for r in rep.results if r.table == "bad" and not r.ok)
