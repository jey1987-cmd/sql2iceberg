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
CASTS = {"snowflake": "FLOAT", "spark": "DOUBLE", "athena": "DOUBLE", "bigquery": "FLOAT64", "sqlite": "REAL"}


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
    raise ValueError(f"unknown target kind {kind!r}: use snowflake, athena or bigquery")


def report_from_config(cfg: dict) -> ValidationReport:
    """Build targets from a plain dict (the CLI's JSON file) and run :func:`validate`.

    {"tables": {"orders": ["order_id", {"name": "amount", "numeric": true}]},
     "reference": {"kind": "snowflake", "name": "source", "tables": {"orders": "DB.SCH.ORDERS"}},
     "targets": [{"kind": "athena", ...}, {"kind": "bigquery", ...}]}
    """
    columns = {t: [ColumnSpec(c) if isinstance(c, str) else ColumnSpec(c["name"], c.get("numeric", False))
                   for c in cols] for t, cols in cfg["tables"].items()}
    with span("validate", tables=len(columns)):
        return validate(columns, _target_from_config(cfg["reference"]),
                        [_target_from_config(t) for t in cfg["targets"]],
                        rel_tol=cfg.get("rel_tol", 1e-9), abs_tol=cfg.get("abs_tol", 1e-6))
