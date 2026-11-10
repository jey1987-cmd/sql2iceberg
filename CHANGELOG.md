# Changelog

## 0.1.0 (first release, alpha)

- Renamed from the working name `sf2iceberg` to `sql2iceberg`.
- Detect SQL platform and SQL in `.sql`, `.py`, `.ipynb`; dialect-aware conversion to Spark SQL with safety findings.
- Connectors: Snowflake (tested live); Postgres, MySQL, SQL Server, Oracle (experimental, mock-tested).
- PySpark job generator: JDBC ingest to Iceberg, curation SQL, hadoop/jdbc/glue/rest/hive catalogs, S3 and GCS warehouses.
- Publishers: Snowflake external Iceberg tables, AWS Glue/Athena, BigQuery BigLake. Plan first, `--apply` to change accounts.
- `validate`: counts, non-null counts and sums across Snowflake, Athena and BigQuery.
- `--trace` JSONL tracer with secret redaction; `doctor` and `issue` commands.
- Live-tested on Snowflake, AWS and GCP with Iceberg format-version 2 (see docs/examples.md).
