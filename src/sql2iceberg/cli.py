"""Command line: sql2iceberg detect | convert | report."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from . import report as rpt
from .convert import UNPARSED, UNSUPPORTED, convert_statement
from .detect import detect_path


def _run(path: str) -> tuple[list, dict]:
    statements = detect_path(path)
    results = {
        s.id: convert_statement(s.sql)
        for s in statements
        if not s.dynamic and not s.parse_error
    }
    return statements, results


def _main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser(prog="sql2iceberg", description=__doc__)
    parser.add_argument("--trace", metavar="FILE", help="write a JSONL trace of this run (secrets are redacted)")
    sub = parser.add_subparsers(dest="cmd", required=True)
    sub.add_parser("doctor", help="check Java, Spark, packages and certificates; print what to put in a bug report")
    iss = sub.add_parser("issue", help="print (or open) a GitHub issue link pre-filled with doctor output")
    iss.add_argument("--title", default="")
    iss.add_argument("--open", action="store_true", help="open the link in your browser")
    va = sub.add_parser("validate", help="compare counts, non-null counts and sums across platforms")
    va.add_argument("config", help="JSON file: tables, reference and targets (see docs/examples.md)")
    va.add_argument("--out", help="write validation.md and validation.json here")
    v = sub.add_parser("verify", help="run one case folder: seed, execute converted SQL in Spark, compare")
    v.add_argument("case", help="case folder with case.json, seed CSVs and expected CSV")
    v.add_argument("--iceberg", action="store_true", help="use an Iceberg Hadoop catalog (needs the Iceberg runtime jar)")
    m = sub.add_parser("map", help="map the tables each statement reads and writes")
    m.add_argument("path")
    m.add_argument("--out", help="write lineage.json here")
    g = sub.add_parser("generate", help="write Iceberg v3 Spark DDL, Snowflake-managed Iceberg DDL and a PySpark script")
    g.add_argument("path", help="code folder (statements to convert)")
    g.add_argument("--ddl", help="SQL file or folder with CREATE TABLE statements (default: same path)")
    g.add_argument("--out", required=True)
    g.add_argument("--external-volume", help="Snowflake external volume name (optional)")
    g.add_argument("--iceberg-version", type=int, default=2, choices=(2, 3))
    pg = sub.add_parser("pyspark", help="generate a PySpark script: JDBC ingest to Iceberg, then curation SQL")
    pg.add_argument("path", help="code folder or file (.sql, .py, .ipynb)")
    pg.add_argument("--out", required=True, help="output folder")
    pg.add_argument("--platform", help="source platform (default: detected from the code)")
    pg.add_argument("--catalog", default="lake")
    pg.add_argument("--namespace", default="db")
    pg.add_argument("--catalog-type", default="hadoop", choices=("hadoop", "glue", "rest", "hive"))
    pg.add_argument("--warehouse", default="./warehouse", help="path or s3://bucket/prefix")
    pg.add_argument("--catalog-uri", default="")
    pg.add_argument("--jdbc-url", default="", help="default: built from the platform's environment variables")
    pg.add_argument("--source-schema", default="")
    pg.add_argument("--iceberg-version", type=int, default=2, choices=(2, 3))
    pu = sub.add_parser("publish", help="expose Iceberg tables to snowflake, aws (Glue) or gcp (BigQuery)")
    pu.add_argument("config", help="JSON file: target, target settings and tables (see docs/publish.md)")
    pu.add_argument("--apply", action="store_true", help="really change the target account (default: print the plan)")
    pu.add_argument("--out", help="write publish_plan.md here")
    c = sub.add_parser("compare", help="compare row counts and estimate cost")
    c.add_argument("--snowflake-counts", required=True, help="CSV with table_name,row_count from Snowflake")
    c.add_argument("--iceberg-counts", required=True, help="CSV with table_name,row_count from Iceberg/Spark")
    c.add_argument("--snowflake-credits", type=float)
    c.add_argument("--credit-price", type=float, help="your price per credit")
    c.add_argument("--spark-seconds", type=float)
    c.add_argument("--spark-hourly-rate", type=float, help="your cost per hour of the Spark cluster")
    c.add_argument("--out")
    q = sub.add_parser("sql", help="print SQL to run in Snowflake: row counts or warehouse credits")
    q.add_argument("--counts", nargs="+", metavar="TABLE")
    q.add_argument("--credits", nargs=3, metavar=("WAREHOUSE", "START", "END"))
    for name in ("detect", "convert", "report"):
        p = sub.add_parser(name)
        p.add_argument("path", help="file or folder (.sql, .py, .ipynb)")
        if name != "detect":
            p.add_argument("--out", help="output folder for report files (default: print)")
            p.add_argument("--fail-on-unsupported", action="store_true")
    args = parser.parse_args(argv)

    if args.cmd == "doctor":
        from . import diagnostics

        result = diagnostics.collect()
        print(diagnostics.to_markdown(result))
        return 1 if any(lvl == "FAIL" for lvl, _ in result["checks"]) else 0

    if args.cmd == "issue":
        from . import diagnostics

        url = diagnostics.issue_url(args.title)
        print(url)
        if args.open:
            import webbrowser

            webbrowser.open(url)
        return 0

    if args.cmd == "validate":
        import json

        from .validate import report_from_config

        rep = report_from_config(json.loads(Path(args.config).read_text(encoding="utf-8")))
        print(rep.to_markdown())
        if args.out:
            Path(args.out).mkdir(parents=True, exist_ok=True)
            (Path(args.out) / "validation.md").write_text(rep.to_markdown(), encoding="utf-8")
            (Path(args.out) / "validation.json").write_text(json.dumps(rep.to_dict(), indent=2, default=str), encoding="utf-8")
        return 0 if rep.ok else 1

    if args.cmd == "verify":
        import json

        from .verify import verify_case

        ver = verify_case(args.case, iceberg=args.iceberg)
        print(json.dumps(ver.to_dict(), indent=2, default=str))
        return 0 if ver.outcome == "pass" else 1

    if args.cmd == "pyspark":
        from .connectors import REGISTRY
        from .platforms import detect_platform
        from .pyspark_gen import MigrationConfig, pyspark_pipeline

        platform = args.platform or detect_platform(args.path)["platform"]
        if not platform:
            print("could not detect the source platform: pass --platform", file=sys.stderr)
            return 2
        cfg = MigrationConfig(
            platform=platform, catalog=args.catalog, namespace=args.namespace, catalog_type=args.catalog_type,
            warehouse=args.warehouse, catalog_uri=args.catalog_uri, jdbc_url=args.jdbc_url,
            source_schema=args.source_schema, format_version=args.iceberg_version,
        )
        stmts = detect_path(args.path, dialect=REGISTRY[platform].sqlglot_dialect)
        script, rep = pyspark_pipeline(stmts, cfg)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "pipeline_pyspark.py").write_text(script, encoding="utf-8")
        bad = [r for r in rep if r["status"] in ("unsupported", "unparsed", "skipped")]
        print(f"platform={platform}; wrote {out / 'pipeline_pyspark.py'}; {len(rep)} statements, {len(bad)} need hand work")
        return 0

    if args.cmd == "publish":
        import json

        from .publish.base import publisher_from_config

        pub, tables = publisher_from_config(json.loads(Path(args.config).read_text(encoding="utf-8")))
        result = pub.apply(tables, confirm=True) if args.apply else pub.plan(tables)
        md = result.to_markdown()
        if args.out:
            Path(args.out).mkdir(parents=True, exist_ok=True)
            (Path(args.out) / "publish_plan.md").write_text(md, encoding="utf-8")
        print(md)
        return 0 if (not args.apply or result.ok) else 1

    if args.cmd == "compare":
        from . import compare as cmp

        counts = cmp.compare_counts(cmp.read_counts(args.snowflake_counts), cmp.read_counts(args.iceberg_counts))
        cost = None
        if any(v is not None for v in (args.snowflake_credits, args.spark_seconds)):
            cost = cmp.cost_summary(args.snowflake_credits, args.credit_price, args.spark_seconds, args.spark_hourly_rate)
        md = cmp.to_markdown(counts, cost)
        if args.out:
            Path(args.out).mkdir(parents=True, exist_ok=True)
            (Path(args.out) / "compare.md").write_text(md, encoding="utf-8")
            print(f"wrote {Path(args.out) / 'compare.md'}")
        else:
            print(md)
        return 0 if all(c["status"] == "match" for c in counts) else 1

    if args.cmd == "sql":
        from . import compare as cmp

        if args.counts:
            print(cmp.count_sql(args.counts))
        if args.credits:
            print(cmp.credits_sql(*args.credits))
        return 0

    if args.cmd == "generate":
        from . import generate as gen

        ddl_text = "\n".join(s.sql.rstrip(";") + ";" for s in detect_path(args.ddl or args.path)
                             if not s.dynamic and not s.parse_error)
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        spark_ddl, w1 = gen.spark_iceberg_ddl(ddl_text, format_version=args.iceberg_version)
        sf_ddl, w2 = gen.snowflake_iceberg_ddl(ddl_text, args.external_volume, iceberg_version=args.iceberg_version)
        (out / "iceberg_spark.sql").write_text(spark_ddl, encoding="utf-8")
        (out / "snowflake_managed_iceberg.sql").write_text(sf_ddl, encoding="utf-8")
        (out / "pipeline_pyspark.py").write_text(gen.pyspark_script(detect_path(args.path)), encoding="utf-8")
        (out / "warnings.txt").write_text("\n".join(w1 + w2) + "\n", encoding="utf-8")
        print(f"wrote 4 files to {out}; {len(w1) + len(w2)} type warnings to review")
        return 0

    statements, results = _run(args.path) if args.cmd != "map" else (detect_path(args.path), {})
    if args.cmd == "map":
        import json

        from .lineage import map_statements

        lin = map_statements(statements)
        text = json.dumps(lin, indent=2)
        if args.out:
            Path(args.out).mkdir(parents=True, exist_ok=True)
            (Path(args.out) / "lineage.json").write_text(text, encoding="utf-8")
        print(f"sources: {lin['source_tables']}\ntargets: {lin['target_tables']}")
        return 0

    if args.cmd == "detect":
        for s in statements:
            tag = "DYNAMIC" if s.dynamic else ("UNPARSED" if s.parse_error else "SQL")
            print(f"{s.id}\t{tag}\t{s.sql[:80].replace(chr(10), ' ')}")
        return 0

    report = rpt.build(statements, results)
    if args.out:
        out = Path(args.out)
        out.mkdir(parents=True, exist_ok=True)
        (out / "report.json").write_text(rpt.to_json(report), encoding="utf-8")
        (out / "report.md").write_text(rpt.to_markdown(report), encoding="utf-8")
        print(f"wrote {out/'report.json'} and {out/'report.md'}")
    else:
        print(rpt.to_markdown(report))
    print(f"summary: {report['summary']}", file=sys.stderr)
    bad = [i for i in report["statements"] if i["status"] in (UNSUPPORTED, UNPARSED)]
    return 1 if (args.fail_on_unsupported and bad) else 0


def main(argv: list[str] | None = None) -> int:
    """Entry point. ``--trace FILE`` is handled here so every command is wrapped in a span."""
    from .tracing import Tracer, set_tracer, span

    argv = list(sys.argv[1:] if argv is None else argv)
    pre = argparse.ArgumentParser(add_help=False)
    pre.add_argument("--trace")
    known, rest = pre.parse_known_args(argv)
    tracer = Tracer(known.trace) if known.trace else None
    if tracer:
        set_tracer(tracer)
    cmd = next((a for a in rest if not a.startswith("-")), "help")
    try:
        with span(f"cli.{cmd}") as sp:
            rc = _main(rest)
            sp.set(exit_code=rc)
    finally:
        if tracer:
            print(tracer.summary(), file=sys.stderr)
            set_tracer(None)
    return rc


if __name__ == "__main__":
    raise SystemExit(main())
