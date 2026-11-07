# Iceberg and Snowflake setup (your machine, your accounts)

Everything here uses made-up data. Nothing runs against Snowflake unless you paste the SQL into your own account.

## 1. Local Iceberg (no cloud)

    ./scripts/setup_local_iceberg.sh

Creates a venv, installs the package, writes seed CSVs and starts Spark with an Iceberg Hadoop catalog in `./warehouse`.
It then tries to create a `format-version=3` table. I could not confirm which Iceberg release supports v3 in Spark
(the spec says v3 exists; the release that first lets Spark write it is unverified), so the script reports the result
instead of assuming. If it fails, set `ICEBERG_PKG` to a newer `iceberg-spark-runtime` and rerun.
The sandbox I built this in could not download the Iceberg jar, so the Iceberg-enabled path is untested there.

## 2. Generate table definitions

    sf2iceberg generate examples/corpus/pipelines --ddl examples/corpus/ddl --out out --external-volume MY_VOLUME

Writes `iceberg_spark.sql` (Iceberg v3, run in Spark), `snowflake_managed_iceberg.sql` (run in Snowflake),
`pipeline_pyspark.py` and `warnings.txt` (column types to review, such as VARIANT).

## 3. Snowflake-managed Iceberg table

The generated statement uses `CATALOG = 'SNOWFLAKE'`, `ICEBERG_VERSION = 3` and an optional `EXTERNAL_VOLUME`
(per Snowflake docs, the volume defaults to `SNOWFLAKE_MANAGED` if omitted; v3 cannot be downgraded). Creating
tables, running queries and loading data spend credits; do that deliberately in your own account.

## 4. Compare counts and cost

    sf2iceberg sql --counts orders customers          # run the output in Snowflake, export as CSV
    sf2iceberg sql --credits MY_WH "2026-11-07 10:00" "2026-11-07 11:00"
    sf2iceberg compare --snowflake-counts sf.csv --iceberg-counts ice.csv \
        --snowflake-credits 1.2 --credit-price 3.00 --spark-seconds 840 --spark-hourly-rate 1.50

`WAREHOUSE_METERING_HISTORY` lags up to 180 minutes (cloud services up to 6 hours), so wait before reading credits.
Cost figures are estimates from rates you supply.
