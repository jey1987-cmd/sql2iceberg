## sql2iceberg 0.1.0 (alpha)

Convert the SQL in your code base to PySpark, persist it as Apache Iceberg, expose the tables to Snowflake, AWS and GCP, and validate the result.

```bash
pip install sql2iceberg                       # core
pip install "sql2iceberg[spark,snowflake,aws,gcp]"
sql2iceberg doctor                            # check your environment
```

### Highlights
- **Detect and convert** SQL from `.sql`, `.py` and `.ipynb` files to Spark SQL, with `converted` / `review` / `unsupported` findings.
- **Generate a PySpark job** that reads the source over JDBC, writes Iceberg to S3 or GCS, and runs the curation SQL.
- **Publish** to Snowflake (external Iceberg tables), AWS Glue/Athena and BigQuery. Plan first, `--apply` to change anything.
- **Validate** counts, non-null counts and sums across platforms in one query per table.
- **Trace** any run with `--trace`, **diagnose** with `doctor`, **report** with `issue`.

### Verified live (synthetic data)
Snowflake source → Iceberg on S3 and GCS → Snowflake, AWS Athena and BigQuery: 38 checks on 4 tables across 4 platforms all matched.
CI is green on Python 3.10, 3.12 and 3.13.

### Not verified yet
Postgres, MySQL, SQL Server and Oracle connectors (mock-tested only); Iceberg format v3 on the targets; Linux and macOS live runs;
Spark 3.5 live runs. See [environments](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/environments.md).

### Docs
[Environments](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/environments.md) ·
[Packages](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/packages.md) ·
[Examples](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/examples.md) ·
[Troubleshooting](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/troubleshooting.md) ·
[Full changelog](https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md)

Found a problem? `sql2iceberg issue --open` pre-fills a GitHub issue with your environment.
