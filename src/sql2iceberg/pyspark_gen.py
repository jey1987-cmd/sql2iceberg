"""Generate a runnable PySpark script: ingest source tables over JDBC into Iceberg, then run the curation SQL.

The script reads credentials from environment variables when it runs. Nothing secret is written into it.
Iceberg table names are ``<catalog>.<namespace>.<table>``; source schema/database qualifiers are dropped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from pathlib import Path

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .connectors import REGISTRY
from .convert import UNPARSED, UNSUPPORTED, convert_statement
from .dataframe_gen import HELPERS, Plan, plan_statement
from .lineage import map_statement, map_statements
from .tracing import span

DEFAULT_ICEBERG_PACKAGE = "org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0"
CATALOG_TYPES = ("hadoop", "glue", "rest", "hive", "jdbc")


@dataclass
class MigrationConfig:
    platform: str = "snowflake"
    catalog: str = "lake"
    namespace: str = "db"
    catalog_type: str = "hadoop"            # hadoop | glue | rest | hive | jdbc (use jdbc, not hadoop, on S3)
    region: str = "us-east-2"               # S3 region when the warehouse is s3://
    gcs_project: str = ""                   # GCP project when the warehouse is gs://
    gcs_token_from_env: bool = False        # gs:// only: read GCS_OAUTH_TOKEN (+ _EXPIRES_AT_MS) at run time, else ADC
    warehouse: str = "./warehouse"          # path or s3://bucket/prefix
    catalog_uri: str = ""                   # rest / hive / jdbc (e.g. jdbc:sqlite:catalog.db)
    format_version: int = 2
    iceberg_package: str = DEFAULT_ICEBERG_PACKAGE
    extra_packages: list[str] = field(default_factory=list)
    jdbc_driver: str = ""                   # default: the connector's driver class
    jdbc_package: str | None = None         # default: the connector's Maven coordinate; "" for none
    jars: list[str] = field(default_factory=list)   # local jars (offline machines); use "" iceberg_package then
    jdbc_url: str = ""                      # default: the connector's own URL, resolved at generation time
    source_schema: str = ""                 # prefix for unqualified source tables, e.g. "PUBLIC"
    # per source table: {"partition_column": "id", "lower": 1, "upper": 1000000, "num_partitions": 8}
    table_options: dict[str, dict] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.catalog_type not in CATALOG_TYPES:
            raise ValueError(f"catalog_type must be one of {CATALOG_TYPES}")
        if self.platform not in REGISTRY:
            raise ValueError(f"unknown platform {self.platform!r}")


def target_name(name: str) -> str:
    """Last part of a table name, lower-cased: the Iceberg table it maps to."""
    return name.split(".")[-1].strip('"`[]').lower()


def _catalog_conf(cfg: MigrationConfig) -> dict[str, str]:
    p = f"spark.sql.catalog.{cfg.catalog}"
    conf = {p: "org.apache.iceberg.spark.SparkCatalog", f"{p}.warehouse": cfg.warehouse}
    if cfg.warehouse.startswith("s3"):
        conf[f"{p}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
        conf[f"{p}.client.region"] = cfg.region
    if cfg.warehouse.startswith("gs"):
        conf[f"{p}.io-impl"] = "org.apache.iceberg.gcp.gcs.GCSFileIO"
        if cfg.gcs_project:
            conf[f"{p}.gcs.project-id"] = cfg.gcs_project
        if cfg.gcs_token_from_env:
            conf[f"{p}.gcs.oauth2.token"] = "$ENV:GCS_OAUTH_TOKEN"
            conf[f"{p}.gcs.oauth2.token-expires-at"] = "$ENV:GCS_OAUTH_TOKEN_EXPIRES_AT_MS"
    if cfg.catalog_type == "jdbc":
        conf[f"{p}.catalog-impl"] = "org.apache.iceberg.jdbc.JdbcCatalog"
        conf[f"{p}.uri"] = cfg.catalog_uri
    elif cfg.catalog_type == "glue":
        conf[f"{p}.catalog-impl"] = "org.apache.iceberg.aws.glue.GlueCatalog"
        conf[f"{p}.io-impl"] = "org.apache.iceberg.aws.s3.S3FileIO"
    else:
        conf[f"{p}.type"] = cfg.catalog_type
        if cfg.catalog_uri:
            conf[f"{p}.uri"] = cfg.catalog_uri
    return conf


def _conf_line(key: str, value: str) -> str:
    if value.startswith("$ENV:"):  # tokens are read when the script runs, never written into it
        return f"    .config({key!r}, os.environ[{value[5:]!r}])"
    return f"    .config({key!r}, {value!r})"


def _qualify(tree: exp.Expression, cfg: MigrationConfig) -> exp.Expression:
    """Point every table (except CTE names) at ``<catalog>.<namespace>.<table>``."""
    ctes = {c.alias for c in tree.find_all(exp.CTE)}

    def fix(node):
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Identifier) and node.name not in ctes:
            node = node.copy()
            node.set("this", exp.to_identifier(target_name(node.name)))
            node.set("db", exp.to_identifier(cfg.namespace))
            node.set("catalog", exp.to_identifier(cfg.catalog))
        return node

    return tree.transform(fix)


def _iceberg_sql(tree: exp.Expression, cfg: MigrationConfig) -> str:
    """Spark SQL text of a qualified tree; CTAS becomes ``CREATE TABLE ... USING iceberg TBLPROPERTIES (...) AS``."""
    text = tree.sql(dialect="spark", comments=False)
    props = f"USING iceberg TBLPROPERTIES ('format-version'='{cfg.format_version}')"
    return re.sub(r"^(CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?\S+)\s+AS\b",
                  lambda m: f"{m.group(1)} {props} AS", text, count=1, flags=re.IGNORECASE)


def rewrite_for_iceberg(spark_sql: str, cfg: MigrationConfig) -> str | None:
    """Point every table at the Iceberg catalog; make CTAS create Iceberg tables.

    Returns None for a plain CREATE TABLE (no query): ingestion or the generated DDL creates those.
    """
    tree = sqlglot.parse_one(spark_sql, read="spark")
    if isinstance(tree, exp.Create) and not tree.expression and (tree.args.get("kind") or "").upper() == "TABLE":
        return None
    return _iceberg_sql(_qualify(tree, cfg), cfg)


def _source_names(statements, cfg: MigrationConfig) -> dict[str, str]:
    """target table -> table name to read over JDBC, for tables the code reads but never creates."""
    out: dict[str, str] = {}
    for name in map_statements(statements)["source_tables"]:
        src = name if "." in name or not cfg.source_schema else f"{cfg.source_schema}.{name}"
        out[target_name(name)] = src
    return out


_HEADER = '''"""Generated by sql2iceberg from {origin}. Review every statement before running it on real data.
{needs}
Set before running: {envs}
Run with: spark-submit {submit}{script_name}
"""
import os

{imports}

CATALOG = {catalog!r}
NAMESPACE = {namespace!r}
FORMAT_VERSION = {format_version!r}
JDBC_URL = {jdbc_url!r}
JDBC_DRIVER = {jdbc_driver!r}
# Iceberg table -> (source table to read, partitioned-read options)
SOURCES = {sources}

spark = (
    SparkSession.builder.appName("sql2iceberg-migration")
{jars_conf}{pkgs_conf}    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
{conf}
    .getOrCreate()
)
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {{CATALOG}}.{{NAMESPACE}}")


def _reader():
    reader = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_DRIVER)
        .option("user", os.environ[{user_env!r}])
    )
    key_file = os.environ.get({key_env!r}) if {key_env!r} else None
    if key_file:  # key-pair login
        reader = reader.option({key_option!r}, key_file)
    else:
        reader = reader.option("password", os.environ[{pw_env!r}])
    return reader


{tz_support}def ingest(target, source, opts):
    """Copy one source table into Iceberg (create or replace)."""
    reader = _reader().option("dbtable", {dbtable})
    if opts.get("partition_column"):
        reader = (
            reader.option("partitionColumn", opts["partition_column"])
            .option("lowerBound", opts["lower"])
            .option("upperBound", opts["upper"])
            .option("numPartitions", opts.get("num_partitions", 4))
        )
    (
        reader.load()
        .writeTo(f"{{CATALOG}}.{{NAMESPACE}}.{{target}}")
        .using("iceberg")
        .tableProperty("format-version", str(FORMAT_VERSION))
        .createOrReplace()
    )


for _target, (_source, _opts) in SOURCES.items():
    ingest(_target, _source, _opts)

{helpers}# ---- curation ----
'''

_SNOWFLAKE_TZ = '''def source_table(source):
    """Spark's JDBC reader cannot read TIMESTAMP_TZ / TIMESTAMP_LTZ: read those columns as UTC TIMESTAMP_NTZ instead."""
    *prefix, table = [p.strip('"') for p in source.split(".")]
    db, schema = (prefix[0], prefix[1]) if len(prefix) == 2 else (None, prefix[0] if prefix else None)
    info = f"{db}.information_schema.columns" if db else "information_schema.columns"
    where = f"table_name = '{table.upper()}' AND table_schema = " + (f"'{schema.upper()}'" if schema else "CURRENT_SCHEMA()")
    columns = (
        _reader()
        .option("query", f"SELECT column_name, data_type FROM {info} WHERE {where} ORDER BY ordinal_position")
        .load()
        .collect()
    )
    if not any(row[1] in ("TIMESTAMP_TZ", "TIMESTAMP_LTZ") for row in columns):
        return source
    def column(name, kind):
        quoted = '"' + name + '"'
        if kind in ("TIMESTAMP_TZ", "TIMESTAMP_LTZ"):
            return f"TO_TIMESTAMP_NTZ(CONVERT_TIMEZONE('UTC', {quoted})) AS {quoted}"
        return quoted

    select = ", ".join(column(name, kind) for name, kind in columns)
    return f"(SELECT {select} FROM {source}) q"


'''


def pyspark_pipeline(statements, cfg: MigrationConfig, dialect: str | None = None, mode: str = "auto") -> tuple[str, list[dict]]:
    """One combined script for all statements. ``mode``: auto | sql | mixed (see ``dataframe_gen``)."""
    with span("pyspark_gen", platform=cfg.platform, catalog_type=cfg.catalog_type, statements=len(statements)):
        sources = _source_names(statements, cfg) if statements else {}
        return _script(statements, cfg, dialect, mode, sources, origin="the scanned code", script_name="pipeline_pyspark.py")


@dataclass
class ScriptInfo:
    """One generated script and what it needs/produces (used to write the organized output folder)."""
    name: str                       # e.g. 01_sales_pyspark.py
    sql_file: str                   # the file it was generated from
    script: str
    report: list[dict]              # one entry per statement: status, strategy, findings, note
    reads: list[str] = field(default_factory=list)     # source tables copied from the source database
    writes: list[str] = field(default_factory=list)    # Iceberg tables it creates or fills
    needs: dict[str, str] = field(default_factory=dict)  # script -> table it provides to this one


def pyspark_outputs(statements, cfg: MigrationConfig, dialect: str | None = None, mode: str = "auto") -> list[ScriptInfo]:
    """One standalone script per source file (``<file>_pyspark.py``), with what each reads, writes and depends on.

    Each script ingests only the source tables its own statements read. Tables that another file creates are read from
    the Iceberg catalog; the script's header names the scripts to run first.
    """
    with span("pyspark_scripts", platform=cfg.platform, statements=len(statements)):
        dialect = dialect or REGISTRY[cfg.platform].sqlglot_dialect
        all_sources = _source_names(statements, cfg) if statements else {}
        lineage = map_statements(statements, dialect) if statements else {"tables": {}, "source_tables": []}
        by_file: dict[str, list] = {}
        for s in statements:
            by_file.setdefault(s.file, []).append(s)
        names: dict[str, str] = {}
        for f in by_file:
            stem = Path(f.split("#")[0]).stem
            name = f"{stem}_pyspark.py"
            n = 2
            while name in names.values():
                name, n = f"{stem}_{n}_pyspark.py", n + 1
            names[f] = name
        file_of = {s.id: s.file for s in statements}
        out: list[ScriptInfo] = []
        for f, stmts in by_file.items():
            reads: set[str] = set()
            writes: set[str] = set()
            for s in stmts:
                if s.dynamic or s.parse_error:
                    continue
                m = map_statement(s.sql, dialect)
                reads.update(m["inputs"])
                writes.update(m["outputs"])
            mine = {target_name(n): all_sources[target_name(n)] for n in reads if target_name(n) in all_sources}
            needs: dict[str, str] = {}
            for n in reads - writes:
                for sid in lineage["tables"].get(n, {}).get("written_by", []):
                    if file_of.get(sid) != f:
                        needs.setdefault(names[file_of[sid]], n)
            script, report = _script(stmts, cfg, dialect, mode, mine, origin=Path(f.split("#")[0]).name,
                                     script_name=names[f], needs=needs)
            out.append(ScriptInfo(names[f], Path(f.split("#")[0]).name, script, report, sorted(mine),
                                  sorted(target_name(w) for w in writes), needs))
        return out


def pyspark_scripts(statements, cfg: MigrationConfig, dialect: str | None = None, mode: str = "auto") -> dict[str, tuple[str, list[dict]]]:
    """Like ``pyspark_outputs`` but as ``{script name: (script text, per-statement report)}``."""
    return {i.name: (i.script, i.report) for i in pyspark_outputs(statements, cfg, dialect, mode)}


def _statement_lines(s, cfg: MigrationConfig, dialect: str, mode: str, ingested: set[str]):
    """Lines of PySpark for one statement, its report entry, and what the script needs (helpers, functions import)."""
    entry = {"id": s.id, "status": "skipped", "note": "", "strategy": "skipped", "findings": []}
    if s.dynamic or s.parse_error:
        entry["note"] = "built from variables" if s.dynamic else "could not parse"
        return [f"# {s.id}: skipped ({entry['note']})", ""], entry, set(), False
    r = convert_statement(s.sql, dialect=dialect)
    entry["status"] = r.status
    entry["findings"] = [{"code": f.code, "severity": f.severity, "message": f.message} for f in r.findings]
    lines = [f"# {s.id} [{r.status}]"]
    lines.extend(f"#   {f.code}: {f.message}" for f in r.findings)
    if r.status in (UNSUPPORTED, UNPARSED) or r.spark_sql is None:
        lines.append("# NOT GENERATED: rewrite by hand")
        lines.append("# " + (r.spark_sql or s.sql).replace("\n", "\n# "))
        return lines + [""], entry, set(), False
    try:
        tree = _qualify(sqlglot.parse_one(r.spark_sql, read="spark"), cfg)
    except ParseError as exc:
        entry["status"], entry["note"] = UNPARSED, str(exc)[:120]
        lines.append(f"# NOT GENERATED: converted SQL did not parse as Spark ({entry['note']})")
        return lines + [""], entry, set(), False
    if isinstance(tree, exp.Create) and not tree.expression and (tree.args.get("kind") or "").upper() == "TABLE":
        table = target_name(tree.this.this.name if isinstance(tree.this, exp.Schema) else tree.this.name)
        if table in ingested:
            entry["note"], entry["strategy"] = "plain CREATE TABLE: the table is ingested from the source", "skipped"
            lines.append("# plain CREATE TABLE skipped: this table is copied from the source by the ingest step")
            return lines + [""], entry, set(), False
        ddl = tree.sql(dialect="spark", comments=False)
        props = f" USING iceberg TBLPROPERTIES ('format-version'='{cfg.format_version}')"
        entry["strategy"] = "spark_sql"
        lines.append("# strategy: Spark SQL (DDL)")
        lines.append(f'spark.sql("""{ddl}{props}""")')
        return lines + [""], entry, set(), False
    sql_text = _iceberg_sql(tree, cfg)
    plan: Plan = plan_statement(tree, sql_text, mode)
    entry["strategy"] = plan.strategy
    entry["df_steps"], entry["sql_steps"] = plan.df_steps, plan.sql_steps
    if plan.strategy == "mixed":
        entry["note"] = f"DataFrame API + Spark SQL ({', '.join(plan.reasons)})"
        lines.append(f"# strategy: mixed, {plan.df_steps} DataFrame step(s) + {plan.sql_steps} Spark SQL step(s) ({', '.join(plan.reasons)})")
    else:
        why = f" ({', '.join(plan.reasons)})" if plan.reasons else ""
        entry["note"] = "Spark SQL" + why
        lines.append(f"# strategy: Spark SQL{why or ' (simple statement)'}")
    lines.extend(plan.lines)
    return lines + [""], entry, plan.helpers, plan.uses_functions


def _script(statements, cfg: MigrationConfig, dialect: str | None, mode: str, sources: dict[str, str], *,
            origin: str, script_name: str, needs: dict[str, str] | None = None) -> tuple[str, list[dict]]:
    """Return (script text, per-statement report). Unsupported or dynamic statements are commented out."""
    if mode not in ("auto", "sql", "mixed"):
        raise ValueError("mode must be auto, sql or mixed")
    conn = REGISTRY[cfg.platform]
    dialect = dialect or conn.sqlglot_dialect
    table_src = {t: (s, cfg.table_options.get(s) or cfg.table_options.get(t) or {}) for t, s in sources.items()}
    jdbc_pkg = conn.jdbc_package if cfg.jdbc_package is None else cfg.jdbc_package
    packages = ",".join(p for p in (cfg.iceberg_package, jdbc_pkg, *cfg.extra_packages) if p)
    jdbc_url = cfg.jdbc_url
    if not jdbc_url:
        try:
            jdbc_url = conn().jdbc_url()
        except KeyError as exc:
            raise ValueError(f"pass --jdbc-url (MigrationConfig.jdbc_url in Python), or set the environment variable {exc.args[0]}") from None
    envs = ", ".join(e for e in (conn.jdbc_user_env, conn.jdbc_password_env) if e)
    if conn.jdbc_key_env:
        envs += f" (or {conn.jdbc_key_env} instead of the password)"
    pkg_arg = f"--packages {packages} " if packages else ""
    joined_jars = ",".join(cfg.jars)
    jars_conf = f"    .config('spark.jars', {joined_jars!r})\n" if cfg.jars else ""
    pkgs_conf = f"    .config('spark.jars.packages', {packages!r})\n" if packages else ""
    body: list[str] = []
    report: list[dict] = []
    helpers: set[str] = set()
    functions = False
    for s in statements:
        lines, entry, used, uses_f = _statement_lines(s, cfg, dialect, mode, set(sources))
        report.append(entry)
        body.extend(lines)
        helpers |= used
        functions = functions or uses_f
    functions = functions or bool(helpers)
    imports = "from pyspark.sql import SparkSession" + (", functions as F" if functions else "")
    helper_text = "".join(HELPERS[h] + "\n\n" for h in sorted(helpers))
    needs_text = ("Run these scripts first (they create tables this one reads): "
                  + ", ".join(f"{n} ({t})" for n, t in sorted(needs.items())) + "\n") if needs else ""
    head = _HEADER.format(
        origin=origin, needs=needs_text, envs=envs, submit=pkg_arg, script_name=script_name, imports=imports,
        catalog=cfg.catalog, namespace=cfg.namespace, format_version=cfg.format_version, jdbc_url=jdbc_url,
        jdbc_driver=cfg.jdbc_driver or conn.jdbc_driver, jars_conf=jars_conf, helpers=helper_text,
        tz_support=_SNOWFLAKE_TZ if cfg.platform == "snowflake" else "", dbtable="source_table(source)" if cfg.platform == "snowflake" else "source",
        sources="{\n" + "".join(f"    {t!r}: ({s!r}, {o!r}),\n" for t, (s, o) in table_src.items()) + "}",
        pkgs_conf=pkgs_conf, user_env=conn.jdbc_user_env, pw_env=conn.jdbc_password_env,
        key_env=conn.jdbc_key_env, key_option=conn.jdbc_key_option,
        conf="\n".join(_conf_line(k, v) for k, v in _catalog_conf(cfg).items()),
    )
    return head + "\n".join(body) + "\nspark.stop()\n", report
