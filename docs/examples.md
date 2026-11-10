# Examples from the live tests

These are the steps that were run for real on 2026-11-09 (Snowflake source, Iceberg on S3 and GCS, three target
platforms). Account IDs, bucket names and project names are replaced with placeholders. Versions: [environments.md](environments.md).

Test data: three source tables in Snowflake (`CUSTOMERS` 20 rows, `ORDERS` 100, `ORDER_ITEMS` 100) and one curation query
that joins them into `curated_orders` (75 rows). The query is `examples/corpus/pipelines/07_curated_orders.sql`.

## 0. Check the environment

```bash
sql2iceberg doctor
```
Prints Python, Java (`PATH` and `JAVA_HOME`), PySpark, package versions, local jars and the names (never values) of the
environment variables that are set. Fix any `WARN` using [troubleshooting.md](troubleshooting.md).

## 1. Detect and convert

```bash
sql2iceberg detect examples/corpus/pipelines
sql2iceberg convert examples/corpus/pipelines --out out
```
`report.md` lists each statement as `converted`, `review` (works, but may behave differently from Snowflake), `unsupported`
(known not to run in Spark) or `unparsed`/`dynamic`. On the synthetic corpus of 39 statements, 4 were flagged for hand work.

## 2. Generate the PySpark job (Iceberg on S3)

```python
from sql2iceberg.detect import detect_path
from sql2iceberg.pyspark_gen import MigrationConfig, pyspark_pipeline

cfg = MigrationConfig(
    platform="snowflake", namespace="live", format_version=2,
    catalog_type="jdbc", catalog_uri="jdbc:sqlite:catalog.db",           # not Hadoop: S3 has no atomic rename
    warehouse="s3://<bucket>/warehouse", region="us-east-2",
    jdbc_url="jdbc:snowflake://<account>.snowflakecomputing.com/?warehouse=<WH>&role=<ROLE>",
    source_schema="<DB>.<SCHEMA>",
    jars=["jars/iceberg-spark-runtime-4.1_2.13-1.12.0.jar", "jars/iceberg-aws-bundle-1.12.0.jar",
          "jars/sqlite-jdbc-3.46.1.3.jar", "jars/snowflake-jdbc-3.19.0.jar"],
    iceberg_package="", jdbc_package="",                                  # use local jars instead of Maven
)
script, report = pyspark_pipeline(detect_path("07_curated_orders.sql", dialect="snowflake"), cfg)
open("pipeline_pyspark.py", "w").write(script)
```

Run it with Java 17, `SPARK_HOME` unset, and the login in the environment (key-pair shown):

```bash
export SNOWFLAKE_USER=... SNOWFLAKE_PRIVATE_KEY_PATH=/path/rsa_key.p8 AWS_PROFILE=...
python pipeline_pyspark.py
```
Result: `customers`, `orders`, `order_items` copied from Snowflake over JDBC into Iceberg tables under
`s3://<bucket>/warehouse/live/`, and `curated_orders` created by the converted SQL.

For the GCS copy: `warehouse="gs://<bucket>/warehouse"`, `gcs_project="<project>"`, `gcs_token_from_env=True`, the
`iceberg-gcp-bundle` jar, and `export GCS_OAUTH_TOKEN="$(gcloud auth print-access-token)"` plus
`GCS_OAUTH_TOKEN_EXPIRES_AT_MS` (now + under an hour, in milliseconds) just before the run.

## 3. Expose the tables

Read each plan first; nothing changes without `--apply`.

### Snowflake (reusing an existing external volume and catalog integration)

`publish_snowflake.json`:
```json
{"target": "snowflake",
 "snowflake": {"database": "<DB>", "schema": "<SCHEMA>", "base_url": "s3://<bucket>/warehouse/",
   "external_volume": "<VOLUME>", "catalog_integration": "<INTEGRATION>",
   "create_volume": false, "create_integration": false, "create_schema": false},
 "tables": [{"name": "CUSTOMERS_S3", "namespace": "live", "format_version": 2,
   "metadata_location": "s3://<bucket>/warehouse/live/customers/metadata/00000-<uuid>.metadata.json"}]}
```
```bash
sql2iceberg publish publish_snowflake.json            # prints the plan
sql2iceberg publish publish_snowflake.json --apply    # runs it
```
Generated SQL (one per table):
```sql
CREATE ICEBERG TABLE IF NOT EXISTS <DB>.<SCHEMA>.CUSTOMERS_S3
  EXTERNAL_VOLUME = '<VOLUME>'
  CATALOG = '<INTEGRATION>'
  METADATA_FILE_PATH = 'live/customers/metadata/00000-<uuid>.metadata.json';
```
Table locations come from the catalog: `tables_from_jdbc_catalog("catalog.db", "live")`.
Row counts returned by the plan's check step: `CUSTOMERS_S3 20`, `ORDERS_S3 100`, `ORDER_ITEMS_S3 100`, `CURATED_ORDERS_S3 75`.

### AWS Glue and Athena

```python
from sql2iceberg.publish import GluePublisher, GlueTarget, tables_from_jdbc_catalog
target = GlueTarget(database="sf2ice_live", region="us-east-2", spark_glue_catalog="glue",
                    athena_output="s3://<bucket>/athena-results/")
# `spark` is a SparkSession with a Glue catalog named "glue" (GlueCatalog + S3FileIO, iceberg-aws-bundle jar)
GluePublisher(target, spark=spark, session=boto3.Session(profile_name="<profile>")).apply(tables, confirm=True)
```
Registers each table with Iceberg's `register_table` procedure, checks Glue reports `table_type = ICEBERG`, then counts with
Athena. Result: `curated_orders 75`, `customers 20`, `order_items 100`, `orders 100`.

### BigQuery (BigLake over the GCS copy)

One-time setup (needs billing):
```bash
gcloud services enable bigqueryconnection.googleapis.com --project <project>
gcloud storage buckets create gs://<bucket> --project <project> --location=US --uniform-bucket-level-access
bq mk --connection --location=US --connection_type=CLOUD_RESOURCE <connection>
# take serviceAccountId from `bq show --connection <project>.us.<connection>` and grant it read access:
gcloud storage buckets add-iam-policy-binding gs://<bucket> --member="serviceAccount:<that id>" --role=roles/storage.objectViewer
```
Generated SQL:
```sql
CREATE OR REPLACE EXTERNAL TABLE `<project>.<dataset>.customers`
  WITH CONNECTION `<project>.us.<connection>`
  OPTIONS (format = 'ICEBERG', uris = ['gs://<bucket>/warehouse/live/customers/metadata/00000-<uuid>.metadata.json']);
```

## 4. Validate

`validate.json`:
```json
{"tables": {"orders": ["order_id", {"name": "amount", "numeric": true}, "status"]},
 "reference": {"kind": "snowflake", "name": "snowflake_source", "tables": {"orders": "<DB>.CORPUS.ORDERS"}},
 "targets": [
   {"kind": "snowflake", "name": "snowflake_iceberg", "tables": {"orders": "<DB>.ICE.ORDERS_S3"}},
   {"kind": "athena", "name": "aws_athena", "profile": "<profile>", "region": "us-east-2", "output": "s3://<bucket>/athena-results/",
    "database": "sf2ice_live", "tables": {"orders": "orders"}},
   {"kind": "bigquery", "name": "gcp_bigquery", "project": "<project>", "tables": {"orders": "`<project>.<dataset>.orders`"}}]}
```
```bash
sql2iceberg validate validate.json --out out --trace out/trace.jsonl
```
For each table and platform it compares `COUNT(*)`, the non-null count of every listed column and the sum of every column
marked `numeric` (relative tolerance 1e-9). Exit code 0 only if everything matches.

**Actual result of the live run: PASS**, 38 checks on 4 tables across 4 platforms (Athena and BigQuery sums differ from Snowflake only in the last floating-point digit, within tolerance).

| table | check | snowflake_source | snowflake_iceberg_s3 | aws_athena | gcp_bigquery | result |
|---|---|---|---|---|---|---|
| customers | row_count | 20.0 | 20.0 | 20.0 | 20.0 | ok |
| customers | non_null:customer_id | 20.0 | 20.0 | 20.0 | 20.0 | ok |
| customers | sum:customer_id | 210.0 | 210.0 | 210.0 | 210.0 | ok |
| customers | non_null:name | 20.0 | 20.0 | 20.0 | 20.0 | ok |
| customers | non_null:country | 20.0 | 20.0 | 20.0 | 20.0 | ok |
| orders | row_count | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| orders | non_null:order_id | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| orders | sum:order_id | 5050.0 | 5050.0 | 5050.0 | 5050.0 | ok |
| orders | non_null:customer_id | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| orders | sum:customer_id | 979.0 | 979.0 | 979.0 | 979.0 | ok |
| orders | non_null:order_ts | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| orders | non_null:amount | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| orders | sum:amount | 15247.76 | 15247.76 | 15247.760000000004 | 15247.760000000004 | ok |
| orders | non_null:status | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| order_items | row_count | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| order_items | non_null:order_id | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| order_items | sum:order_id | 5050.0 | 5050.0 | 5050.0 | 5050.0 | ok |
| order_items | non_null:sku | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| order_items | non_null:qty | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| order_items | sum:qty | 200.0 | 200.0 | 200.0 | 200.0 | ok |
| order_items | non_null:price | 100.0 | 100.0 | 100.0 | 100.0 | ok |
| order_items | sum:price | 2562.28 | 2562.28 | 2562.28 | 2562.28 | ok |
| curated_orders | row_count | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | non_null:order_id | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | sum:order_id | 3747.0 | 3747.0 | 3747.0 | 3747.0 | ok |
| curated_orders | non_null:status | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | non_null:amount | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | sum:amount | 10615.26 | 10615.26 | 10615.260000000002 | 10615.260000000002 | ok |
| curated_orders | non_null:customer_id | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | sum:customer_id | 725.0 | 725.0 | 725.0 | 725.0 | ok |
| curated_orders | non_null:customer_name | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | non_null:country | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | non_null:item_lines | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | sum:item_lines | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | non_null:units | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | sum:units | 157.0 | 157.0 | 157.0 | 157.0 | ok |
| curated_orders | non_null:items_value | 75.0 | 75.0 | 75.0 | 75.0 | ok |
| curated_orders | sum:items_value | 4214.54 | 4214.54 | 4214.54 | 4214.54 | ok |

## 5. Trace

```bash
sql2iceberg --trace out/trace.jsonl publish publish_snowflake.json --apply
```
One JSON line per step: `name`, `parent`, `duration_ms`, `status`, redacted `attrs`. The live validation run recorded one `validate.query` span per table and platform (with `platform`, `table`, `checks`). Checks are
batched into one SELECT per table, which cut the run from about 150 queries to 16; BigQuery calls through the `bq` CLI take
about 20 s each because of CLI start-up.

## 6. Open an issue with your environment

```bash
sql2iceberg issue --title "Glue register fails on existing table" --open
```
