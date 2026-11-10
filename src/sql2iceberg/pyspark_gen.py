"""Generate a runnable PySpark script: ingest source tables over JDBC into Iceberg, then run the curation SQL.

The script reads credentials from environment variables when it runs. Nothing secret is written into it.
Iceberg table names are ``<catalog>.<namespace>.<table>``; source schema/database qualifiers are dropped.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

from .connectors import REGISTRY
from .convert import UNPARSED, UNSUPPORTED, convert_statement
from .lineage import map_statements
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


def rewrite_for_iceberg(spark_sql: str, cfg: MigrationConfig) -> str | None:
    """Point every table at the Iceberg catalog; make CTAS create Iceberg tables.

    Returns None for a plain CREATE TABLE (no query): ingestion or the generated DDL creates those.
    """
    tree = sqlglot.parse_one(spark_sql, read="spark")
    if isinstance(tree, exp.Create) and not tree.expression and (tree.args.get("kind") or "").upper() == "TABLE":
        return None
    ctes = {c.alias for c in tree.find_all(exp.CTE)}

    def fix(node):
        if isinstance(node, exp.Table) and isinstance(node.this, exp.Identifier) and node.name not in ctes:
            node = node.copy()
            node.set("this", exp.to_identifier(target_name(node.name)))
            node.set("db", exp.to_identifier(cfg.namespace))
            node.set("catalog", exp.to_identifier(cfg.catalog))
        return node

    text = tree.transform(fix).sql(dialect="spark", comments=False)
    props = f"USING iceberg TBLPROPERTIES ('format-version'='{cfg.format_version}')"
    return re.sub(r"^(CREATE (?:OR REPLACE )?TABLE (?:IF NOT EXISTS )?\S+)\s+AS\b",
                  lambda m: f"{m.group(1)} {props} AS", text, count=1, flags=re.IGNORECASE)


def _source_names(statements, cfg: MigrationConfig) -> dict[str, str]:
    """target table -> table name to read over JDBC, for tables the code reads but never creates."""
    out: dict[str, str] = {}
    for name in map_statements(statements)["source_tables"]:
        src = name if "." in name or not cfg.source_schema else f"{cfg.source_schema}.{name}"
        out[target_name(name)] = src
    return out


_HEADER = '''"""Generated by sql2iceberg. Review every statement before running it on real data.

Set before running: {envs}
Run with: spark-submit {submit}pipeline_pyspark.py
"""
import os

from pyspark.sql import SparkSession

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


def ingest(target, source, opts):
    """Copy one source table into Iceberg (create or replace)."""
    reader = (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("driver", JDBC_DRIVER)
        .option("dbtable", source)
        .option("user", os.environ[{user_env!r}])
    )
    key_file = os.environ.get({key_env!r}) if {key_env!r} else None
    if key_file:  # key-pair login
        reader = reader.option({key_option!r}, key_file)
    else:
        reader = reader.option("password", os.environ[{pw_env!r}])
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

# ---- curation ----
'''


def pyspark_pipeline(statements, cfg: MigrationConfig, dialect: str | None = None) -> tuple[str, list[dict]]:
    with span("pyspark_gen", platform=cfg.platform, catalog_type=cfg.catalog_type, statements=len(statements)):
        return _pyspark_pipeline(statements, cfg, dialect)


def _pyspark_pipeline(statements, cfg: MigrationConfig, dialect: str | None = None) -> tuple[str, list[dict]]:
    """Return (script text, per-statement report). Unsupported or dynamic statements are commented out."""
    conn = REGISTRY[cfg.platform]
    dialect = dialect or conn.sqlglot_dialect
    sources = _source_names(statements, cfg) if statements else {}
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
    lines = [_HEADER.format(
        envs=envs, submit=pkg_arg, catalog=cfg.catalog, namespace=cfg.namespace,
        format_version=cfg.format_version, jdbc_url=jdbc_url, jdbc_driver=cfg.jdbc_driver or conn.jdbc_driver,
        jars_conf=jars_conf,
        sources="{\n" + "".join(f"    {t!r}: ({s!r}, {o!r}),\n" for t, (s, o) in table_src.items()) + "}",
        pkgs_conf=pkgs_conf, user_env=conn.jdbc_user_env, pw_env=conn.jdbc_password_env,
        key_env=conn.jdbc_key_env, key_option=conn.jdbc_key_option,
        conf="\n".join(_conf_line(k, v) for k, v in _catalog_conf(cfg).items()),
    )]
    report: list[dict] = []
    for s in statements:
        entry = {"id": s.id, "status": "skipped", "note": ""}
        report.append(entry)
        if s.dynamic or s.parse_error:
            entry["note"] = "built from variables" if s.dynamic else "could not parse"
            lines.append(f"# {s.id}: skipped ({entry['note']})")
            continue
        r = convert_statement(s.sql, dialect=dialect)
        entry["status"] = r.status
        lines.append(f"# {s.id} [{r.status}]")
        lines.extend(f"#   {f.code}: {f.message}" for f in r.findings)
        if r.status in (UNSUPPORTED, UNPARSED) or r.spark_sql is None:
            lines.append("# NOT GENERATED: rewrite by hand")
            lines.append("# " + (r.spark_sql or s.sql).replace("\n", "\n# "))
            continue
        try:
            sql = rewrite_for_iceberg(r.spark_sql, cfg)
        except ParseError as exc:
            entry["status"], entry["note"] = UNPARSED, str(exc)[:120]
            lines.append(f"# NOT GENERATED: converted SQL did not parse as Spark ({entry['note']})")
            continue
        if sql is None:
            entry["note"] = "plain CREATE TABLE: table is created by ingestion"
            lines.append("# plain CREATE TABLE skipped: ingestion or generated DDL creates it")
        else:
            lines.append(f'spark.sql("""{sql}""")')
        lines.append("")
    lines.append("spark.stop()")
    return "\n".join(lines) + "\n", report
