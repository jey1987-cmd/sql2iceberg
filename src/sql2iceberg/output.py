"""Write the organized output folder for ``sql2iceberg pyspark``.

    out/
      README.md              what was generated, which script to run in which order, what still needs your attention
      run_all.py             runs every script in the right order
      scripts/<file>_pyspark.py   one PySpark script per SQL file
      reports/conversion_report.md / .json   every statement: status, approach, findings
"""

from __future__ import annotations

import json
from pathlib import Path

import sqlglot
from sqlglot import exp

STRATEGY_TEXT = {
    "spark_sql": "Spark SQL",
    "mixed": "DataFrame steps + Spark SQL",
    "skipped": "not generated",
}
STATUS_TEXT = {
    "converted": "converted",
    "review": "converted, please review",
    "unsupported": "NOT converted: needs hand work",
    "unparsed": "NOT converted: could not read the SQL",
    "skipped": "NOT converted: dynamic or unreadable SQL",
}

PLAIN = {
    "DIVIDE": "dividing by zero gives NULL in Spark but an error in Snowflake",
    "DATEADD": "adding days to a timestamp can drop the time of day in Spark; check timestamp columns",
    "REGEXP_LIKE": "the pattern must match the whole text in Snowflake but only part of it in Spark",
    "TO_DATE_FORMAT": "the date format was simplified; check that bad dates behave as you expect",
    "VARIANT_PATH": "assumes the column holds JSON text (that is how it is copied over JDBC)",
    "FLATTEN_JSON": "assumes the array column holds JSON text; KEY and PATH of FLATTEN are not supported",
    "OBJECT_CONSTRUCT": "became a Spark STRUCT; null handling and dynamic keys differ",
    "DROPPED": "part of the SQL could not be expressed in Spark and was left out; read the report",
}

_RUN_ALL = '''"""Run every generated script in the right order:  python run_all.py   (or: python run_all.py NAME ...)

Needs the same environment as a single script: Java 17, the SNOWFLAKE_* (or other source database) variables set,
and PySpark installed. Stops at the first script that fails.
"""
import subprocess
import sys
from pathlib import Path

ORDER = {order!r}
scripts = Path(__file__).parent / "scripts"
chosen = sys.argv[1:] or ORDER
for name in chosen:
    print(f"--- running {{name}}", flush=True)
    code = subprocess.run([sys.executable, str(scripts / name)]).returncode
    if code:
        sys.exit(f"{{name}} failed (exit code {{code}}). Fix it, then run it again; scripts before it already finished.")
print("all scripts finished")
'''


def validate_config(statements, cfg, dialect: str) -> dict | None:
    """A ready-to-run config for ``sql2iceberg validate``: your original SELECTs on the source vs the Iceberg tables.

    Nothing is created in the source database: each original SELECT is only queried. Returns None when no statement
    is a CREATE TABLE AS SELECT or INSERT ... SELECT/VALUES.
    """
    from .pyspark_gen import target_name

    queries: dict[str, list[str]] = {}
    for s in statements:
        if s.dynamic or s.parse_error:
            continue
        try:
            tree = sqlglot.parse_one(s.sql, read=dialect)
        except sqlglot.errors.ParseError:
            continue
        if isinstance(tree, exp.Create) and tree.expression is not None and (tree.args.get("kind") or "").upper() == "TABLE":
            target, query = tree.this, tree.expression.copy()
        elif isinstance(tree, exp.Insert) and tree.expression is not None:
            target, query = tree.this, tree.expression.copy()
            if tree.args.get("with_") is not None and query.args.get("with_") is None:
                query.set("with_", tree.args["with_"].copy())
        else:
            continue
        if isinstance(target, exp.Schema):
            target = target.this
        if not isinstance(target, exp.Table):
            continue
        ctes = {c.alias for c in query.find_all(exp.CTE)}
        if cfg.source_schema:
            def qualify(node, ctes=ctes):
                if isinstance(node, exp.Table) and isinstance(node.this, exp.Identifier) and not node.args.get("db")                         and node.name not in ctes:
                    node = node.copy()
                    schema, *rest = cfg.source_schema.split(".")[::-1]
                    node.set("db", exp.to_identifier(schema))
                    if rest:
                        node.set("catalog", exp.to_identifier(rest[0]))
                return node
            query = query.transform(qualify)
        text = query.sql(dialect=dialect, comments=False)
        queries.setdefault(target_name(target.name), []).append(f"SELECT * FROM ({text})" if isinstance(query, exp.Values) else text)
    if not queries:
        return None
    packages = [cfg.iceberg_package] if cfg.iceberg_package else []
    return {
        "tables": "auto",
        "reference": {"kind": cfg.platform, "name": f"{cfg.platform}_source", "queries": queries},
        "targets": [{"kind": "spark", "name": "spark_iceberg", "catalog": cfg.catalog, "catalog_type": "hadoop",
                     "warehouse": cfg.warehouse, "jars": cfg.jars, "packages": packages,
                     "tables": {t: f"{cfg.catalog}.{cfg.namespace}.{t}" for t in queries}}],
    }


def run_order(infos) -> list[str]:
    """Script names ordered so that a script runs after the scripts that create tables it reads."""
    names = [i.name for i in infos]
    needs = {i.name: set(i.needs) & set(names) for i in infos}
    ordered: list[str] = []
    while len(ordered) < len(names):
        ready = [n for n in names if n not in ordered and needs[n] <= set(ordered)]
        if not ready:                       # circular dependency: keep the remaining order as found
            ready = [n for n in names if n not in ordered]
        ordered.append(ready[0])
    return ordered


def _hand_work(infos) -> list[tuple[str, dict]]:
    return [(i.name, r) for i in infos for r in i.report if r["status"] in ("unsupported", "unparsed", "skipped")]


def _review(infos) -> list[tuple[str, dict]]:
    return [(i.name, r) for i in infos for r in i.report if r["status"] == "review"]


def _short(statement_id: str) -> str:
    return statement_id.split("#")[0].replace("\\", "/").split("/")[-1]


def conversion_report_md(infos) -> str:
    lines = [
        "# Conversion report",
        "",
        ("One section per SQL statement. **Approach** says how the PySpark code does the work: plain Spark SQL, or DataFrame steps "
         "mixed with Spark SQL. **Findings** are things to check against your original results."),
        "",
    ]
    for info in infos:
        lines += [f"## {info.sql_file}  ->  scripts/{info.name}", ""]
        for r in info.report:
            lines.append(f"### `{r['id']}`")
            lines.append(f"- Status: {STATUS_TEXT.get(r['status'], r['status'])}")
            lines.append(f"- Approach: {STRATEGY_TEXT.get(r['strategy'], r['strategy'])}" + (f" ({r['note']})" if r["note"] else ""))
            for f in r.get("findings", []):
                lines.append(f"- {f['severity'].upper()} {f['code']}: {f['message']}")
            lines.append("")
    return "\n".join(lines)


def readme_md(infos, platform: str, mode: str, catalog: str, namespace: str, warehouse: str) -> str:
    order = run_order(infos)
    by_name = {i.name: i for i in infos}
    hand, review = _hand_work(infos), _review(infos)
    total = sum(len(i.report) for i in infos)
    lines = [
        "# Generated PySpark scripts", "",
        (f"{len(infos)} SQL file(s), {total} statement(s) converted from **{platform}** SQL to PySpark that writes **Apache Iceberg** "
         f"tables (`{catalog}.{namespace}.<table>` under `{warehouse}`)."), "",
        "## What is in this folder", "",
        "| Item | What it is |", "|---|---|",
        ("| `scripts/` | One PySpark script per SQL file. Each one is complete on its own: it copies the tables it needs from the "
         "source database into Iceberg, then runs the transformation and writes the result as an Iceberg table. |"),
        "| `run_all.py` | Runs all scripts in the right order. |",
        "| `reports/conversion_report.md` | Every statement, how it was converted, and what to double-check. |",
        "| `reports/conversion_report.json` | The same information for tools. |",
        ("| `reports/validate.json` | A ready-made check: runs your original SELECTs on the source database and compares row counts, "
         "non-empty counts and column sums with the Iceberg tables. |"), "",
        "## How to run", "",
        "1. Install what the scripts need once: Python 3.10+, Java 17 and `pip install pyspark`. (`sql2iceberg doctor` checks this.)",
        "2. Give the scripts your source database login through environment variables (the exact names are at the top of every script).",
        "3. Run everything: `python run_all.py`. Or run one script: `python scripts/<name>_pyspark.py`.",
        ("4. Check the results (same login variables; run it from the folder you ran the scripts from, so the warehouse path is found): "
         "`sql2iceberg validate reports/validate.json --out reports`. It writes `reports/validation.md`."), "",
        "## Scripts, in the order to run them", "",
        "| # | Script | From SQL file | How it works | Copies from source | Writes Iceberg table(s) | Run after |",
        "|---|---|---|---|---|---|---|"]
    for n, name in enumerate(order, 1):
        i = by_name[name]
        kinds = sorted({STRATEGY_TEXT.get(r["strategy"], r["strategy"]) for r in i.report if r["strategy"] != "skipped"})
        lines.append(f"| {n} | `{name}` | {i.sql_file} | {' and '.join(kinds) or 'nothing generated'} | "
                     f"{', '.join(i.reads) or '-'} | {', '.join(i.writes) or '-'} | {', '.join(sorted(i.needs)) or '-'} |")
    lines += ["", ("**How it works:** *Spark SQL* means the script runs the converted query as SQL (used for simple queries). "
                   "*DataFrame steps + Spark SQL* means each step of a complex query (each CTE) is a PySpark DataFrame, "
                   "built with the DataFrame API where possible and with SQL where that is the clearest way; the result is the same."), ""]
    if hand:
        lines += ["## Needs your attention (not converted)", ""]
        lines += [f"- `{_short(r['id'])}` in `{name}`: " + "; ".join(f"{f['code']}: {f['message']}" for f in r.get("findings", []))
                  for name, r in hand]
        lines.append("")
    if review:
        lines += ["## Converted, but please check", "",
                  ("These were converted, but may behave slightly differently in Spark than in your source database. "
                   "Compare their output with your original results before relying on them."), ""]
        lines += [f"- `{_short(r['id'])}` in `{name}`: " + "; ".join(f"{f['code']} ({PLAIN.get(f['code'], f['message'])})" for f in r.get("findings", []))
                  for name, r in review]
        lines.append("")
    if not hand and not review:
        lines += ["## Needs your attention", "",
                  ("Nothing: every statement was converted without findings. Still compare the results "
                   "with your original queries (the `sql2iceberg validate` command does that)."), ""]
    lines += [f"Generated with mode `{mode}`. Re-run `sql2iceberg pyspark` to regenerate; edits you make in this folder are overwritten.", ""]
    return "\n".join(lines)


def write_output_folder(infos, out: Path, *, platform: str, mode: str, catalog: str, namespace: str, warehouse: str,
                        combined: str | None = None, validate: dict | None = None) -> list[Path]:
    """Create ``out/`` with scripts/, reports/, README.md and run_all.py. Returns the files written."""
    out = Path(out)
    (out / "scripts").mkdir(parents=True, exist_ok=True)
    (out / "reports").mkdir(parents=True, exist_ok=True)
    written: list[Path] = []

    def put(path: Path, text: str) -> None:
        path.write_text(text, encoding="utf-8")
        written.append(path)

    for i in infos:
        put(out / "scripts" / i.name, i.script)
    if combined is not None:
        put(out / "scripts" / "pipeline_pyspark.py", combined)
    put(out / "run_all.py", _RUN_ALL.format(order=run_order(infos)))
    put(out / "reports" / "conversion_report.md", conversion_report_md(infos))
    put(out / "reports" / "conversion_report.json", json.dumps(
        [{"script": i.name, "sql_file": i.sql_file, "reads": i.reads, "writes": i.writes, "needs": i.needs,
          "statements": i.report} for i in infos], indent=2))
    if validate is not None:
        put(out / "reports" / "validate.json", json.dumps(validate, indent=2))
    put(out / "README.md", readme_md(infos, platform, mode, catalog, namespace, warehouse))
    return written
