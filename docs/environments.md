# Environments

## Requirements

| Component | Required | Notes |
|---|---|---|
| Python | 3.10 or newer | declared in `pyproject.toml`; live-tested on 3.13 |
| Java | 17 (21 should work) for PySpark 4; 8, 11 or 17 for PySpark 3.5 | `JAVA_HOME` must point at it. `sql2iceberg doctor` warns if `JAVA_HOME` and `java` on `PATH` differ |
| PySpark | 3.5 or 4.x | live-tested on 4.1.4. Do not set `SPARK_HOME` to a different Spark |
| Iceberg Spark runtime jar | must match your Spark and Scala version | tested: `iceberg-spark-runtime-4.1_2.13-1.12.0.jar` |
| Iceberg cloud bundles | `iceberg-aws-bundle` for S3 and Glue; `iceberg-gcp-bundle` for GCS | tested: 1.12.0 both |
| JDBC driver for the source | one jar per source platform | tested: `snowflake-jdbc-3.19.0.jar`; `sqlite-jdbc-3.46.1.3.jar` for the local catalog and unit tests |
| Network | Maven Central (or local jars), your cloud endpoints | behind a proxy that re-signs TLS see [troubleshooting](troubleshooting.md#certificate_unknown--ssl-errors) |

The jars can come from Maven (`--packages`, `spark.jars.packages`) or from local files (`MigrationConfig.jars`).
On locked-down networks download them once and use local files.

## Authentication prerequisites (you do these before running)

| Platform | What to set up | Variables / tools read |
|---|---|---|
| Snowflake | a user with key-pair (recommended), password or SSO; a role and warehouse | `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_USER`, `SNOWFLAKE_PRIVATE_KEY_PATH` or `SNOWFLAKE_PASSWORD` or `SNOWFLAKE_AUTHENTICATOR`, `SNOWFLAKE_ROLE`, `SNOWFLAKE_WAREHOUSE` |
| Snowflake to S3 | an external volume with a storage integration and IAM role that Snowflake can assume | created once in your account; the library can create it if you pass `aws_role_arn` |
| AWS | profile or environment credentials | `AWS_PROFILE`, `AWS_REGION`, optionally `AWS_CA_BUNDLE` |
| GCP | `gcloud auth login`; a project with **billing enabled**; BigQuery Connection API enabled | `gcloud` credentials; a short-lived token is passed to Spark through `GCS_OAUTH_TOKEN` |
| Postgres, MySQL, SQL Server, Oracle (experimental) | native client environment variables | `PGHOST` ..., `MYSQL_HOST` ..., `MSSQL_SERVER` ..., `ORACLE_DSN` ... |

## Supported environments

| Area | Verified live | Written, mock-tested only | Not supported yet |
|---|---|---|---|
| Source SQL platform | Snowflake | Postgres, MySQL, SQL Server (T-SQL), Oracle | others |
| Spark | PySpark 4.1.4 (local mode) | PySpark 3.5 | Databricks, EMR, Dataproc submit helpers |
| Iceberg catalog | JDBC catalog (SQLite file), Hadoop (local), Glue (for `register_table`) | REST, Hive | Nessie, Polaris |
| Object storage | S3 (us-east-2), GCS (US) | | Azure Blob / ADLS |
| Target: Snowflake | external Iceberg tables via object-store catalog integration, existing external volume | Glue-integration mode, creating volumes and integrations | Snowflake-managed Iceberg writes from Spark |
| Target: AWS | Glue Data Catalog registration and Athena queries | | Redshift Spectrum, Lake Formation |
| Target: GCP | BigQuery BigLake Iceberg tables over GCS | | BigQuery Omni on S3 |
| Iceberg format version | 2 on all three targets; 3 on a Snowflake external table over S3 (one table: counts and sums matched) | 3 on Athena and BigQuery (generated, unverified) | |
| Operating system | Windows 10 | CI on Ubuntu (unit tests) | macOS |

## What was tested

The live run on 2026-11-09 used a synthetic data set ("corpus": customers, orders, order items) and these exact versions.

| Item | Version / value |
|---|---|
| OS | Windows 10 (10.0.19045), Git Bash and PowerShell |
| Python | 3.13.17 |
| Java | Eclipse Temurin 17.0.21 |
| PySpark | 4.1.4 (Spark local mode) |
| Iceberg | 1.12.0 (`spark-runtime-4.1_2.13`, `aws-bundle`, `gcp-bundle`) |
| SQLGlot | 30.30.0 |
| snowflake-connector-python | 5.2.0 |
| Snowflake JDBC | 3.19.0 |
| Snowflake | one account on AWS us-east-2, role SYSADMIN, key-pair login, XSMALL warehouse |
| AWS | S3, Glue Data Catalog, Athena workgroup `primary`, region us-east-2, boto3 1.43.127, AWS CLI 2.30.6 |
| GCP | one project, BigQuery location `US`, GCS location `US`, Cloud SDK 556.0.0 |

Results (all matched the Snowflake source): 20 customers, 100 orders, 100 order items, 75 curated orders on
Snowflake (Iceberg on S3), AWS Athena and BigQuery (Iceberg on GCS). Step by step with commands:
[examples.md](examples.md).

**Version 0.2.0 (2026-11-10)**, same machine and account: three sets of made-up queries over the same tables, converted, run as one script per SQL file with `run_all.py` and checked with the generated `validate.json` (original SELECTs on Snowflake against the local Iceberg tables): 10 basic queries 84 of 84 checks matched, 8 INSERT statements 32 of 32, 10 complex queries (CTEs, windows, PIVOT, LISTAGG, FLATTEN, recursive CTE, VARIANT paths) 99 of 100. The INSERT and complex sets were also run from the package installed from PyPI into a fresh virtual environment, with the same results, but only after pinning PySpark to 4.1.4: an unpinned install pulled PySpark 4.3.0, which does not work with the Spark 4.1 Iceberg jar (fixed by limiting the `spark` extra to PySpark below 4.2 in 0.2.1). The one difference is the precision of a windowed `AVG` (Snowflake truncates to 5 decimals, Spark keeps 6). The FLATTEN and VARIANT queries ran on columns that are all NULL in that account, so they prove the code runs, not the values.

The unit test suite (`pytest -m "not spark"`, 100 tests) needs no cloud accounts. One test runs the generated PySpark
job against SQLite with a local Iceberg catalog (`SQL2ICE_ICEBERG=1` and the jars in `./jars`).
