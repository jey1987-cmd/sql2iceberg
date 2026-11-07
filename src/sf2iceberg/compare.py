"""Compare row counts and cost between Snowflake and Iceberg runs.

Counts: two CSVs (table,row_count) that you export. Cost: Snowflake credits come from your own
ACCOUNT_USAGE views (up to ~3 hours late); Spark cost is runtime x a rate you supply. Costs are estimates.
"""

from __future__ import annotations

import csv
from pathlib import Path

SNOWFLAKE_COUNT_SQL = "SELECT '{t}' AS table_name, COUNT(*) AS row_count FROM {t};"

# Credits for one warehouse in a time window. ACCOUNT_USAGE data lags (up to 180 minutes; cloud services up to 6 h).
SNOWFLAKE_CREDITS_SQL = """SELECT SUM(credits_used_compute) AS compute_credits,
       SUM(credits_used_cloud_services) AS cloud_services_credits,
       SUM(credits_used) AS credits_used   -- may be higher than billed (cloud services adjustment)
FROM snowflake.account_usage.warehouse_metering_history
WHERE warehouse_name = '{warehouse}'
  AND start_time >= '{start}'::timestamp_ltz AND end_time <= '{end}'::timestamp_ltz;"""


def count_sql(tables: list[str]) -> str:
    return "\nUNION ALL\n".join(SNOWFLAKE_COUNT_SQL.format(t=t).rstrip(";") for t in tables) + ";"


def credits_sql(warehouse: str, start: str, end: str) -> str:
    return SNOWFLAKE_CREDITS_SQL.format(warehouse=warehouse, start=start, end=end)


def read_counts(path: str | Path) -> dict[str, int]:
    with open(path, newline="", encoding="utf-8") as fh:
        rows = list(csv.DictReader(fh))
    return {r["table_name"].strip().lower().split(".")[-1]: int(r["row_count"]) for r in rows}


def spark_counts(spark, tables: list[str]) -> dict[str, int]:
    return {t.lower().split(".")[-1]: spark.table(t).count() for t in tables}


def compare_counts(snowflake: dict[str, int], iceberg: dict[str, int]) -> list[dict]:
    out = []
    for t in sorted(set(snowflake) | set(iceberg)):
        a, b = snowflake.get(t), iceberg.get(t)
        status = "missing" if a is None or b is None else ("match" if a == b else "mismatch")
        out.append({"table": t, "snowflake": a, "iceberg": b, "status": status})
    return out


def cost_summary(snowflake_credits: float | None, credit_price: float | None,
                 spark_seconds: float | None, spark_hourly_rate: float | None) -> dict:
    sf = round(snowflake_credits * credit_price, 4) if snowflake_credits is not None and credit_price is not None else None
    sp = round(spark_seconds / 3600 * spark_hourly_rate, 4) if spark_seconds is not None and spark_hourly_rate is not None else None
    return {
        "snowflake_credits": snowflake_credits, "snowflake_cost": sf,
        "spark_seconds": spark_seconds, "spark_cost": sp,
        "note": "estimate: Snowflake from credits x your price; Spark from runtime x your hourly rate; "
                "storage, network and engineer time are not included",
    }


def to_markdown(counts: list[dict], cost: dict | None) -> str:
    lines = ["# Count and cost comparison", "", "| table | snowflake | iceberg | status |", "|---|---|---|---|"]
    lines += [f"| {c['table']} | {c['snowflake']} | {c['iceberg']} | {c['status']} |" for c in counts]
    if cost:
        lines += ["", "## Cost (estimate)", "", f"- Snowflake: {cost['snowflake_credits']} credits, cost {cost['snowflake_cost']}",
                  f"- Spark: {cost['spark_seconds']} s, cost {cost['spark_cost']}", f"- {cost['note']}"]
    return "\n".join(lines) + "\n"
