## sql2iceberg 0.1.3

```bash
pip install --upgrade sql2iceberg
```

### Fixed
- Command-line errors from missing settings (for example `PGHOST` not set, or a missing key in a publish or validate config) now
  print a one-line `error: ...` message and exit with status 2 instead of a Python traceback. Set `SQL2ICEBERG_DEBUG=1` for the
  full traceback when you report a bug.

### Documentation
- Corrected the Iceberg v3 statements. Format-version 2 was tested on Snowflake, Athena and BigQuery. Version 3 was also verified
  once: a Spark-written v3 table on S3 was read by a Snowflake external Iceberg table, and counts and sums matched the managed
  table. Athena and BigQuery on v3 remain untested.

Built for any SQL code base; tested end to end on Snowflake. Postgres, MySQL, SQL Server and Oracle are experimental.

**Full changelog:** https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md
