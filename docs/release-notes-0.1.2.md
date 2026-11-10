## sql2iceberg 0.1.2

Fixes a command that did not work in 0.1.0 and 0.1.1, and makes the CLI usable for databases other than Snowflake.

```bash
pip install --upgrade sql2iceberg
```

### Fixed
- `sql2iceberg pyspark --catalog-type jdbc` was rejected with "invalid choice", although the README and troubleshooting guide
  recommend a JDBC catalog for S3 and GCS warehouses. It now works.
- The `publish` command's help pointed at a docs page that does not exist.

### Added
- **`--platform`** on `detect`, `convert`, `report` and `map` (`snowflake`, `postgres`, `mysql`, `mssql`, `oracle`). It is detected
  from your code when omitted. Before this, these commands always parsed SQL as Snowflake.
- **`pyspark` options** that were only available from Python: `--region`, `--jars` and `--no-maven` (offline machines),
  `--jdbc-driver`, `--gcs-project`, `--gcs-token-from-env`.
- **`validate`** can use Postgres, MySQL, SQL Server and Oracle as the reference or a target, not only Snowflake, Athena and BigQuery.
- Regression tests that run the documented quick-start commands (80 tests in total).

### Still true
Built for any SQL code base; tested end to end on **Snowflake** (Snowflake source, Iceberg on S3 and GCS, readers on Snowflake, AWS
Athena and BigQuery). Postgres, MySQL, SQL Server and Oracle are experimental: the generated scripts were checked, but they were not
run against live servers. Alpha software: verify results on your own data.

**Full changelog:** https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md ·
[Troubleshooting](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/troubleshooting.md) ·
[Examples](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/examples.md)
