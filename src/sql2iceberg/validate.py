"""Validate a migration: compare row counts, per-column non-null counts and numeric sums across platforms.

Each platform is a ``Target``: a name, a function that runs SQL and returns rows, and a map from the logical
table name to that platform's qualified name. One of the targets is the reference (usually the source system).
Nothing here connects anywhere by itself; ``snowflake_runner``, ``athena_runner`` and ``bigquery_runner`` are
thin adapters over clients you already have.

What it proves: same row count, same count of non-null values per column, same sum of each numeric column
(within a tolerance). What it does not prove: row-by-row equality. For that use ``sql2iceberg verify`` on a case
folder, or compare a sample you export yourself.
"""

from __future__ import annotations

import math
from collections.abc import Callable
from dataclasses import dataclass, field

from .tracing import span

#: how each platform spells a floating-point cast
CASTS = {"snowflake": "FLOAT", "spark": "DOUBLE", "athena": "DOUBLE", "bigquery": "FLOAT64", "sqlite": "REAL",
         # experimental sources (not run against live servers)
         "postgres": "DOUBLE PRECISION", "mysql": "DOUBLE", "mssql": "FLOAT", "oracle": "BINARY_DOUBLE"}


@dataclass
class Target:
    name: str
    run: Callable[[str], list[tuple]]
    tables: dict[str, str]                 # logical name -> qualified name on this platform
    cast: str = "DOUBLE"                   # floating-point type name, see CASTS


@dataclass
class ColumnSpec:
    name: str
    numeric: bool = False


@dataclass
class CheckResult:
    table: str
    check: str                             # row_count | non_null:<col> | sum:<col>
    values: dict[str, float | int | None]  # platform -> value
    ok: bool
    note: str = ""


@dataclass
class ValidationReport:
    reference: str
    results: list[CheckResult] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors and all(r.ok for r in self.results)

    def to_dict(self) -> dict:
        return {"reference": self.reference, "ok": self.ok, "errors": self.errors,
                "results": [r.__dict__ for r in self.results]}

    def to_markdown(self) -> str:
        names = list(self.results[0].values) if self.results else []
        head = ["table", "check", *names, "result"]
        out = [f"# Validation (reference: {self.reference}): {'PASS' if self.ok else 'FAIL'}\n",
               "| " + " | ".join(head) + " |", "|" + "---|" * len(head)]
        for r in self.results:
            vals = ["" if r.values.get(n) is None else str(r.values[n]) for n in names]
            out.append("| " + " | ".join([r.table, r.check, *vals, "ok" if r.ok else f"MISMATCH {r.note}"]) + " |")
        out += [f"\nERROR: {e}" for e in self.errors]
        return "\n".join(out) + "\n"


def _close(a, b, rel: float, abs_: float) -> bool:
    if a is None or b is None:
        return a is None and b is None
    return math.isclose(float(a), float(b), rel_tol=rel, abs_tol=abs_)


def validate(columns: dict[str, list[ColumnSpec]], reference: Target, others: list[Target], *,
             rel_tol: float = 1e-9, abs_tol: float = 1e-6) -> ValidationReport:
    """Compare every table in ``columns`` (logical name -> columns to check) across the targets."""
    report = ValidationReport(reference.name)
    targets = [reference, *others]
    for table, cols in columns.items():
        checks = [("row_count", "COUNT(*)", False)]
        for c in cols:
            checks.append((f"non_null:{c.name}", f"COUNT({c.name})", False))
            if c.numeric:
                checks.append((f"sum:{c.name}", f"SUM(CAST({c.name} AS {{cast}}))", True))
        values: dict[str, dict[str, float | None]] = {t.name: {} for t in targets}
        for t in targets:
            qualified = t.tables.get(table)
            if not qualified:
                report.errors.append(f"{t.name}: no table mapped for {table}")
                continue
            # one query per table and platform: every check is a column of the same SELECT
            # zero-padded aliases: clients that return columns sorted by name still give the right order
            exprs = ", ".join(f"{expr.format(cast=t.cast)} AS c{i:03d}" for i, (_, expr, _) in enumerate(checks))
            with span("validate.query", platform=t.name, table=table, checks=len(checks)):
                try:
                    rows = t.run(f"SELECT {exprs} FROM {qualified}")
                    row = rows[0] if rows else ()
                    if len(row) != len(checks):
                        raise ValueError(f"expected {len(checks)} columns, got {len(row)}")
                    for (label, _, _), v in zip(checks, row, strict=True):
                        values[t.name][label] = float(v) if v is not None else None
                except Exception as exc:  # noqa: BLE001  report per target, keep checking the others
                    report.errors.append(f"{t.name} {table}: {type(exc).__name__}: {str(exc)[:200]}")
        for label, _, is_sum in checks:
            vals = {t.name: values[t.name].get(label) for t in targets}
            ref = vals[reference.name]
            bad = [t.name for t in others if t.name in vals and not _close(
                ref, vals[t.name], rel_tol if is_sum else 0, abs_tol if is_sum else 0)]
            report.results.append(CheckResult(table, label, vals, not bad, f"differs on {', '.join(bad)}" if bad else ""))
    return report


# ---- adapters ---------------------------------------------------------------------------------------------

def snowflake_runner(connector) -> Callable[[str], list[tuple]]:
    return connector.query


def athena_runner(session, workgroup: str, output: str, database: str | None = None) -> Callable[[str], list[tuple]]:
    import time

    athena = session.client("athena")

    def run(sql: str) -> list[tuple]:
        kw = {"QueryExecutionContext": {"Database": database}} if database else {}
        qid = athena.start_query_execution(QueryString=sql, WorkGroup=workgroup,
                                           ResultConfiguration={"OutputLocation": output}, **kw)["QueryExecutionId"]
        while True:
            st = athena.get_query_execution(QueryExecutionId=qid)["QueryExecution"]["Status"]
            if st["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
                break
            time.sleep(1)
        if st["State"] != "SUCCEEDED":
            raise RuntimeError(f"Athena {st['State']}: {st.get('StateChangeReason', '')}")
        rows = athena.get_query_results(QueryExecutionId=qid)["ResultSet"]["Rows"][1:]
        return [tuple(c.get("VarCharValue") for c in r["Data"]) for r in rows]

    return run


def bigquery_runner(client) -> Callable[[str], list[tuple]]:
    return lambda sql: [tuple(r) for r in client.query(sql).result()]


def get_connector_for(kind: str):
    from .connectors import get_connector

    return get_connector(kind)


# ---- config-file entry point ------------------------------------------------------------------------------

def _target_from_config(cfg: dict) -> Target:
    kind = cfg["kind"]
    if kind == "snowflake":
        from .connectors import get_connector

        conn = get_connector("snowflake")
        return Target(cfg.get("name", "snowflake"), snowflake_runner(conn), cfg["tables"], CASTS["snowflake"])
    if kind == "athena":
        import boto3

        session = boto3.Session(profile_name=cfg.get("profile"), region_name=cfg.get("region"))
        run = athena_runner(session, cfg.get("workgroup", "primary"), cfg["output"], cfg.get("database"))
        return Target(cfg.get("name", "athena"), run, cfg["tables"], CASTS["athena"])
    if kind == "bigquery":
        from google.cloud import bigquery

        client = bigquery.Client(project=cfg["project"], location=cfg.get("location", "US"))
        return Target(cfg.get("name", "bigquery"), bigquery_runner(client), cfg["tables"], CASTS["bigquery"])
    from .connectors import REGISTRY

    if kind in REGISTRY:  # postgres, mysql, mssql, oracle (experimental): credentials come from environment variables
        conn = get_connector_for(kind)
        return Target(cfg.get("name", kind), conn.query, cfg["tables"], CASTS[kind])
    raise ValueError(f"unknown target kind {kind!r}: use snowflake, postgres, mysql, mssql, oracle, athena or bigquery")


def _spark_target(cfg: dict, spark=None):
    """A Spark/Iceberg target. Builds a local session from the config unless one is passed in."""
    if spark is None:
        from pyspark.sql import SparkSession

        catalog = cfg.get("catalog", "lake")
        prefix = f"spark.sql.catalog.{catalog}"
        builder = (SparkSession.builder.appName("sql2iceberg-validate")
                   .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                   .config(prefix, "org.apache.iceberg.spark.SparkCatalog")
                   .config(f"{prefix}.type", cfg.get("catalog_type", "hadoop"))
                   .config(f"{prefix}.warehouse", cfg.get("warehouse", "./warehouse")))
        if cfg.get("jars"):
            builder = builder.config("spark.jars", ",".join(cfg["jars"]))
        if cfg.get("packages"):
            builder = builder.config("spark.jars.packages", ",".join(p for p in cfg["packages"] if p))
        for key, value in (cfg.get("conf") or {}).items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()
        spark.sparkContext.setLogLevel("ERROR")
    run = lambda sql: [tuple(r) for r in spark.sql(sql).collect()]
    return spark, Target(cfg.get("name", "spark_iceberg"), run, cfg["tables"], CASTS["spark"])


_NUMERIC_TYPES = ("byte", "short", "int", "long", "float", "double", "decimal")


def report_from_config(cfg: dict, spark=None) -> ValidationReport:
    """Build targets from a plain dict (the CLI's JSON file) and run :func:`validate`.

    {"tables": {"orders": ["order_id", {"name": "amount", "numeric": true}]},
     "reference": {"kind": "snowflake", "name": "source", "tables": {"orders": "DB.SCH.ORDERS"}},
     "targets": [{"kind": "athena", ...}, {"kind": "bigquery", ...}]}

    Two conveniences used by the folder ``sql2iceberg pyspark`` writes:

    * a target ``{"kind": "spark", "warehouse": ..., "catalog": "lake", "tables": {"orders": "lake.db.orders"}}`` checks the
      Iceberg tables with Spark (``spark`` can be an existing session);
    * ``"tables": "auto"`` takes the columns to check from that Spark target, and a reference with
      ``"queries": {"orders": ["SELECT ..."]}`` runs your original SELECTs (several are combined with UNION ALL) on the source,
      so you can compare a generated table with what the SQL returns without creating anything in the source database.
    """
    spark_session, targets = spark, []
    for t in cfg["targets"]:
        if t["kind"] == "spark":
            spark_session, target = _spark_target(t, spark_session)
        else:
            target = _target_from_config(t)
        targets.append(target)
    spark_target = next((t for t, c in zip(targets, cfg["targets"], strict=True) if c["kind"] == "spark"), None)
    if cfg["tables"] == "auto":
        if spark_target is None:
            raise ValueError('"tables": "auto" needs a target with "kind": "spark"')
        columns = {name: [ColumnSpec(f.name, f.dataType.typeName() in _NUMERIC_TYPES)
                          for f in spark_session.table(qualified).schema.fields]
                   for name, qualified in spark_target.tables.items()}
    else:
        columns = {t: [ColumnSpec(c) if isinstance(c, str) else ColumnSpec(c["name"], c.get("numeric", False))
                       for c in cols] for t, cols in cfg["tables"].items()}
    ref_cfg = dict(cfg["reference"])
    if "queries" in ref_cfg:
        ref_cfg["tables"] = {
            table: "(" + " UNION ALL ".join(f"SELECT * FROM ({q})" for q in queries) + ") AS q("
                   + ", ".join(c.name for c in columns[table]) + ")"
            for table, queries in ref_cfg.pop("queries").items() if table in columns}
    with span("validate", tables=len(columns)):
        return validate(columns, _target_from_config(ref_cfg), targets,
                        rel_tol=cfg.get("rel_tol", 1e-9), abs_tol=cfg.get("abs_tol", 1e-6))
