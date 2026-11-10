# Changelog

All notable changes to this project are documented here. The format follows [Keep a Changelog](https://keepachangelog.com/),
and the project uses [Semantic Versioning](https://semver.org/). While the version is below 1.0, minor releases may change behaviour.

## [0.1.3] - 2026-11-10

### Fixed
- Command-line errors from missing settings (for example `PGHOST` not set, or a missing key in a publish or validate config) now print a
  one-line `error: ...` message and exit with status 2 instead of a Python traceback. Set `SQL2ICEBERG_DEBUG=1` to get the traceback.

### Documentation
- Corrected the Iceberg v3 statements. Format-version 2 was tested on Snowflake, Athena and BigQuery. Version 3 was also verified
  once: a Spark-written v3 table on S3 was read by a Snowflake external Iceberg table, and counts and sums matched the managed table.
  Athena and BigQuery on v3 remain untested. The Snowflake publisher's v3 warning now says this.

## [0.1.2] - 2026-11-10

### Fixed
- `sql2iceberg pyspark --catalog-type jdbc` was rejected ("invalid choice") even though the README and troubleshooting guide
  recommend it for S3 and GCS warehouses. `jdbc` is now accepted.
- The `publish` command's help pointed at a docs page that does not exist; it now points at `docs/examples.md`.

### Added
- `sql2iceberg pyspark` options that were only available from Python: `--region`, `--jars` and `--no-maven` (offline machines),
  `--jdbc-driver`, `--gcs-project` and `--gcs-token-from-env` (GCS warehouses).
- `--platform` on `detect`, `convert`, `report` and `map`. The platform (snowflake, postgres, mysql, mssql, oracle) is detected
  from the code when omitted. Before this, these commands always parsed SQL as Snowflake.
- `validate` can use Postgres, MySQL, SQL Server and Oracle (experimental) as the reference or a target, not only Snowflake,
  Athena and BigQuery.
- Regression tests that run the documented quick-start commands.

## [0.1.1] - 2026-11-09

Documentation-only release: improves the PyPI project page. No code changes.

### Added
- Logo banner and status badges (PyPI version, Python versions, CI, license) at the top of the README.
- Table of contents in the README.

### Fixed
- README links to the docs, changelog, contributing guide and license were relative, so they did not resolve on the PyPI
  page. They are now absolute GitHub URLs.

## [0.1.0] - 2026-11-09

First public release (alpha). Published on PyPI as [`sql2iceberg`](https://pypi.org/project/sql2iceberg/).

### Added
- **Detection and conversion.** Find SQL in `.sql`, `.py` and `.ipynb` files; guess the source platform from driver imports,
  connection strings, SQL syntax and environment variables; convert to Spark SQL with SQLGlot (Snowflake, Postgres, MySQL,
  T-SQL, Oracle dialects) and flag output that cannot run or may behave differently (`converted`, `review`, `unsupported`).
- **Connectors.** Snowflake (tested live); Postgres, MySQL, SQL Server and Oracle (experimental, mock-tested only). Credentials come
  from environment variables only.
- **PySpark job generator.** JDBC read of source tables, write to Iceberg, then the curation SQL with tables pointed at the
  Iceberg catalog. Catalog types `hadoop`, `jdbc`, `glue`, `rest`, `hive`; S3 and GCS warehouses; Snowflake key-pair login.
  Generated scripts never contain secrets.
- **Publishers.** Expose Iceberg tables to Snowflake (external Iceberg tables, reusing or creating the volume and catalog
  integration), AWS (Glue registration, Athena counts) and GCP (BigQuery BigLake). Every publisher prints a plan first and only
  changes an account with `--apply`.
- **Validation.** `sql2iceberg validate` compares row counts, per-column non-null counts and numeric sums across platforms in
  one batched query per table.
- **Tracer.** `--trace FILE` writes timed, nested JSONL spans for every command, with secrets redacted.
- **Support tooling.** `sql2iceberg doctor` checks Java, Spark, packages and certificates; `sql2iceberg issue` opens a GitHub issue
  pre-filled with environment names and versions (never values). Issue templates, troubleshooting guide, environment and
  package lists, and worked examples from the live tests are in `docs/`.
- **pytest plugin** with Spark fixtures (`sql2iceberg[pytest]`).

### Changed
- Renamed from the working name `sf2iceberg` to `sql2iceberg` (package, CLI, plugin, docs). If you installed the old name,
  `pip uninstall sf2iceberg` first (see troubleshooting).
- Default Iceberg format version is 2, the version verified on all three target platforms.

### Verified
Live run on 2026-11-09 with synthetic data: Snowflake source to Iceberg on S3 (JDBC, key-pair login) and on GCS, exposed to
Snowflake, AWS Athena and BigQuery. 38 validation checks across 4 tables and 4 platforms matched (20 customers, 100 orders,
100 order items, 75 curated orders). CI passes on Python 3.10, 3.12 and 3.13 (Linux).

### Known limitations
- Only Snowflake was exercised as a source. Postgres, MySQL, SQL Server and Oracle are untested.
- Tested on Iceberg format version 2; v3 support on Snowflake and BigQuery is unverified.
- BigQuery cannot read an S3 warehouse; the GCP path writes a second copy to GCS.
- Snowflake object-store tables and BigQuery tables point at one metadata file and need a refresh after new writes.
- Validation proves counts, non-null counts and sums, not row-by-row equality.
- Live tests ran on Windows 10 with Python 3.13 only.

[0.1.3]: https://github.com/jey1987-cmd/sql2iceberg/releases/tag/v0.1.3
[0.1.2]: https://github.com/jey1987-cmd/sql2iceberg/releases/tag/v0.1.2
[0.1.1]: https://github.com/jey1987-cmd/sql2iceberg/releases/tag/v0.1.1
[0.1.0]: https://github.com/jey1987-cmd/sql2iceberg/releases/tag/v0.1.0
