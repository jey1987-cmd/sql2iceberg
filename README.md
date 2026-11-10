<p align="center">
  <img src="https://raw.githubusercontent.com/jey1987-cmd/sql2iceberg/main/docs/assets/logo.png" alt="sql2iceberg: SQL to PySpark to Apache Iceberg" width="520">
</p>

<p align="center">
  <a href="https://pypi.org/project/sql2iceberg/"><img alt="PyPI" src="https://img.shields.io/pypi/v/sql2iceberg"></a>
  <a href="https://pypi.org/project/sql2iceberg/"><img alt="Python versions" src="https://img.shields.io/pypi/pyversions/sql2iceberg"></a>
  <a href="https://github.com/jey1987-cmd/sql2iceberg/actions/workflows/ci.yml"><img alt="CI" src="https://github.com/jey1987-cmd/sql2iceberg/actions/workflows/ci.yml/badge.svg"></a>
  <a href="https://github.com/jey1987-cmd/sql2iceberg/blob/main/LICENSE"><img alt="License: MIT" src="https://img.shields.io/pypi/l/sql2iceberg"></a>
</p>

# sql2iceberg

Take the SQL in your code base, convert it to **PySpark**, persist the result as **Apache Iceberg** tables, expose those
tables to **Snowflake, AWS (Glue/Athena) and GCP (BigQuery)**, and **validate** the migration.

**Built for any SQL code base, tested end to end on Snowflake.** Snowflake, Postgres, MySQL, SQL Server and Oracle SQL are
parsed and converted (any dialect SQLGlot supports can be added). The full path, source to Iceberg to three target platforms, was
run against real Snowflake, AWS and Google Cloud accounts; the other databases are experimental.

**Status: alpha.** One source platform (Snowflake) and three target platforms were exercised against real
accounts; everything else is written but untested and labelled so. See [what was tested](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/environments.md#what-was-tested)
and [what is not supported yet](#limitations).

```
your code ──detect──▶ SQL ──convert──▶ Spark SQL ──generate──▶ PySpark job ──run──▶ Iceberg on S3 / GCS
                                                                                      │
                          validate ◀──── counts, nulls, sums ────────────────────────┤
                                                                                      ├─▶ Snowflake (external Iceberg table)
                                                                                      ├─▶ AWS Glue / Athena
                                                                                      └─▶ BigQuery (BigLake)
```

## Table of contents

- [Core capabilities](#core-capabilities)
- [Install](#install)
- [Environment dependencies (summary)](#environment-dependencies-summary)
- [Quick start](#quick-start)
- [Python API](#python-api)
- [Documentation](#documentation)
- [Getting help and reporting problems](#getting-help-and-reporting-problems)
- [Limitations](#limitations)
- [License](#license)

## Core capabilities

| # | Capability | Command / API | Status |
|---|---|---|---|
| 1 | **Detect** the SQL platform and find SQL in `.sql`, `.py`, `.ipynb` | `detect`, `detect_platform()` | Snowflake tested; other dialects parse via SQLGlot |
| 2 | **Connect** to the source (you set up authentication first) | `connectors.get_connector()` | Snowflake tested live; Postgres, MySQL, SQL Server, Oracle **experimental** (mock-tested only) |
| 3 | **Convert** to Spark SQL and flag what cannot be trusted | `convert`, `convert_statement()` | tested |
| 4 | **Generate a PySpark job**: JDBC read, write Iceberg, run the curation SQL | `pyspark`, `pyspark_pipeline()` | tested end to end (Snowflake to Iceberg on S3 and on GCS) |
| 5 | **Expose** Iceberg tables to other platforms | `publish`, `publish.*Publisher` | Snowflake, AWS Glue/Athena, BigQuery all tested live |
| 6 | **Validate** counts, non-null counts and column sums across platforms | `validate`, `validate.validate()` | tested live across four platforms |
| 7 | **Report**: converted PySpark, plan of changes, validation results (Markdown and JSON) | all commands | tested |
| 8 | **Trace** every step with timings, secrets redacted | `--trace trace.jsonl`, `tracing.span()` | tested |
| 9 | **Diagnose** your environment; open a pre-filled GitHub issue | `doctor`, `issue` | tested |

Safety defaults: `publish` prints a plan and changes nothing unless you pass `--apply`; credentials are only read from
environment variables or your cloud profile; generated scripts never contain secrets.

## Install

```bash
pip install sql2iceberg                       # core: detect + convert (needs only sqlglot)
pip install "sql2iceberg[spark]"              # run the generated PySpark jobs locally
pip install "sql2iceberg[snowflake,aws,gcp]"  # connectors and publishers you need
pip install "sql2iceberg[all]"                # spark + snowflake + aws + gcp
pip install "sql2iceberg[pytest]"             # pytest plugin and fixtures
```

Extras: `spark`, `snowflake`, `postgres`, `mysql`, `mssql`, `oracle`, `aws`, `gcp`, `pytest`, `all`, `dev`.

## Environment dependencies (summary)

| Needed for | Requirement |
|---|---|
| everything | Python 3.10 or newer |
| running generated Spark jobs | **Java 17** (Java 21 should work), `pyspark` 3.5 or 4.x. Do not set `SPARK_HOME` to another Spark. |
| Iceberg tables | Iceberg Spark runtime jar matching your Spark (we used `iceberg-spark-runtime-4.1_2.13-1.12.0`) |
| Iceberg on S3 / GCS | `iceberg-aws-bundle` / `iceberg-gcp-bundle` jars |
| reading the source over JDBC | the JDBC driver jar (we used `snowflake-jdbc-3.19.0`) |
| Snowflake | account, role that can create external volumes, catalog integrations and Iceberg tables; a warehouse |
| AWS | an AWS profile or environment credentials with S3, Glue and (to count) Athena permissions |
| GCP | `gcloud auth login`, a project **with billing enabled**, a BigQuery connection to the bucket |
| behind a TLS-inspecting proxy | a CA bundle for Python and a Java truststore: see [troubleshooting](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/troubleshooting.md) |

Full details, versions we tested and the list of every package and jar: [docs/environments.md](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/environments.md) and
[docs/packages.md](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/packages.md). Run `sql2iceberg doctor` to check your machine.

## Quick start

```bash
# 1. which platform is this code base for, and what SQL is in it? (add --platform postgres|mysql|mssql|oracle|snowflake to override)
sql2iceberg detect path/to/code
# 2. convert, and see what needs hand work
sql2iceberg convert path/to/code --out out
# 3. generate the PySpark job (reads the source over JDBC, writes Iceberg, runs the curation SQL)
sql2iceberg pyspark path/to/code --out out --platform snowflake \
    --catalog-type jdbc --catalog-uri jdbc:sqlite:catalog.db \
    --warehouse s3://my-bucket/warehouse --namespace live --iceberg-version 2
# 4. run it (set SNOWFLAKE_USER and SNOWFLAKE_PRIVATE_KEY_PATH or SNOWFLAKE_PASSWORD first)
spark-submit out/pipeline_pyspark.py
# 5. expose the tables: read the plan first, then --apply
sql2iceberg publish publish.json
sql2iceberg publish publish.json --apply
# 6. validate across platforms
sql2iceberg validate validate.json --out out
```

Add `--trace trace.jsonl` to any command to record a timed trace of the run.
Worked examples from real runs (configs, plans, results): [docs/examples.md](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/examples.md).

## Python API

```python
from sql2iceberg import detect_path, convert_statement
from sql2iceberg.pyspark_gen import MigrationConfig, pyspark_pipeline
from sql2iceberg.publish import SnowflakePublisher, SnowflakeTarget, tables_from_jdbc_catalog
from sql2iceberg.validate import validate, Target, ColumnSpec

stmts = detect_path("pipelines/", dialect="snowflake")
script, report = pyspark_pipeline(stmts, MigrationConfig(platform="snowflake", warehouse="s3://b/wh",
                                                         catalog_type="jdbc", catalog_uri="jdbc:sqlite:catalog.db"))
```

## Documentation

- [Environments and what was tested](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/environments.md)
- [Every package, jar and service used in this build](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/packages.md)
- [Examples from the live tests](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/examples.md)
- [Troubleshooting frequent problems](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/troubleshooting.md)
- [Known conversion differences between Snowflake and Spark](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/known-issues.md)
- [Roadmap](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/roadmap.md)

## Getting help and reporting problems

- Run `sql2iceberg doctor`, then `sql2iceberg issue --title "short summary" --open` to open a GitHub issue pre-filled with
  your environment (names and versions only, never values or credentials).
- Attach a trace (`--trace trace.jsonl`) after reading it. Secrets are redacted but check anyway.
- Issues: <https://github.com/jey1987-cmd/sql2iceberg/issues>. Please read [troubleshooting](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/troubleshooting.md) first.
- Security problems: do not open a public issue; see [CONTRIBUTING.md](https://github.com/jey1987-cmd/sql2iceberg/blob/main/CONTRIBUTING.md#security).

## Limitations

- Only **Snowflake as a source** and **Snowflake, AWS Glue/Athena, BigQuery as targets** were run against real systems.
  Postgres, MySQL, SQL Server and Oracle connectors, REST/Hive catalogs, Spark 3.5, Azure and Databricks are untested.
- Tested with Iceberg **format-version 2**. Whether Snowflake and BigQuery read v3 tables is unverified here.
- BigQuery cannot read an S3 warehouse. The GCP path writes a second Iceberg copy to GCS.
- Snowflake object-store tables and BigQuery tables point at one metadata file (a snapshot): refresh after new writes.
- Validation compares counts, non-null counts and sums. It does not compare every row.
- Tested on Windows 10 with Python 3.13. Linux and macOS should work; CI covers Linux but live tests were not run there.
- Conversion uses [SQLGlot](https://github.com/tobymao/sqlglot); the converter flags constructs that behave differently in
  Spark (see [known issues](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/known-issues.md)) but a clean result still needs your own verification.

## License

MIT. Not affiliated with Snowflake, the Apache Software Foundation, Amazon or Google. "Iceberg" is a trademark of the ASF.
