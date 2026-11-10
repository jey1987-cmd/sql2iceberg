# Packages, jars and services used in this build

Versions are those used for the live test on 2026-11-09 unless a range is given. Install ranges come from `pyproject.toml`.

## Python packages

### Runtime (installed with `pip install sql2iceberg`)

| Package | Range | Role |
|---|---|---|
| sqlglot | `>=30` (tested 30.30.0) | SQL parsing and dialect conversion |

### Optional extras

| Extra | Packages (range) | Tested version | Role |
|---|---|---|---|
| `spark` | pyspark `>=3.5` | 4.1.4 (with py4j 0.10.9.9) | run generated jobs, verify |
| `snowflake` | snowflake-connector-python `>=3.12` | 5.2.0 | source connector, publish and validate on Snowflake |
| `postgres` | psycopg2-binary `>=2.9` | not tested | Postgres connector (experimental) |
| `mysql` | pymysql `>=1.1` | not tested | MySQL connector (experimental) |
| `mssql` | pyodbc `>=5` | not tested | SQL Server connector (experimental), needs an ODBC driver |
| `oracle` | oracledb `>=2` | not tested | Oracle connector (experimental) |
| `aws` | boto3 `>=1.34` | 1.43.127 (botocore 1.43.127) | Glue verification, Athena queries |
| `gcp` | google-cloud-bigquery `>=3.20` | not tested (the `bq` CLI was used) | BigQuery publisher and validation |
| `pytest` | pytest `>=8`, chispa `>=0.10`, pyspark `>=3.5` | 9.2.0, 0.12.0 | pytest plugin and Spark fixtures |
| `all` | pyspark, snowflake-connector-python, boto3, google-cloud-bigquery | | the cloud and Spark set |
| `dev` | pytest, ruff, build, twine | ruff 0.17.4 | development and release |

### Used by the example scripts only

python-dotenv 1.2.4 (loads `.env`), cryptography 50.0.2 (pulled in by the Snowflake connector for key-pair login).

## Java libraries (jars, not on PyPI)

| Jar | Version | Used for | Source |
|---|---|---|---|
| iceberg-spark-runtime-4.1_2.13 | 1.12.0 | Iceberg tables in Spark 4.1 | Maven Central, `org.apache.iceberg` |
| iceberg-aws-bundle | 1.12.0 | S3FileIO, GlueCatalog | Maven Central, `org.apache.iceberg` |
| iceberg-gcp-bundle | 1.12.0 | GCSFileIO | Maven Central, `org.apache.iceberg` |
| snowflake-jdbc | 3.19.0 | Spark reading Snowflake over JDBC | Maven Central, `net.snowflake` |
| sqlite-jdbc | 3.46.1.3 | JDBC catalog file, unit-test source | Maven Central, `org.xerial` |
| JDBC drivers for other sources (referenced, untested) | postgresql 42.7.4, mysql-connector-j 8.4.0, mssql-jdbc 12.8.1.jre11, ojdbc11 23.5.0.24.07 | other source platforms | Maven Central |

Always check the checksum of a downloaded jar against the `.sha1` file Maven publishes next to it.

## Tools and runtimes

| Tool | Version | Used for |
|---|---|---|
| Python | 3.13.17 (3.10+ supported) | library |
| Java (Eclipse Temurin) | 17.0.21 | Spark |
| AWS CLI | 2.30.6 | checking access, listing S3 and Glue |
| Google Cloud SDK (`gcloud`, `bq`) | 556.0.0 | project, bucket, connection setup; BigQuery statements |
| Git | any recent | |
| keytool (ships with the JDK) | 17.0.21 | building a Java truststore behind a proxy |

## Cloud services used

| Service | What for | Cost note |
|---|---|---|
| Snowflake (XSMALL warehouse, external volume, catalog integration, Iceberg tables) | source data, external Iceberg tables, count queries | warehouse credits for queries; tiny here |
| Amazon S3 | Iceberg data and metadata | storage |
| AWS Glue Data Catalog | table registration | free tier covers this |
| Amazon Athena | row counts | billed per data scanned (10 MB minimum per query) |
| Google Cloud Storage | second Iceberg copy | storage |
| BigQuery (BigLake external tables, BigQuery Connection API) | row counts and queries | billed per bytes processed |

## Standards and formats

Apache Iceberg table format version 2 (tested), version 3 (verified on one Snowflake external table over S3; unverified on Athena and BigQuery), Parquet data
files, JDBC, OAuth2 bearer tokens for GCS access, Snowflake key-pair (PKCS#8) authentication.
