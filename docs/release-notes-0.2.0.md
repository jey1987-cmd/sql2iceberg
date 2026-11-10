## sql2iceberg 0.2.0

```bash
pip install --upgrade "sql2iceberg>=0.2.0"
```

New to this? Read the [step-by-step guide](https://github.com/jey1987-cmd/sql2iceberg/blob/main/docs/getting-started.md): no Spark knowledge needed.

### Changed (read this if you used 0.1.x)
- `sql2iceberg pyspark` now writes **one PySpark script per SQL file** into `out/scripts/`, plus `out/README.md` (run order and what to
  check), `out/run_all.py` and `out/reports/`. The single `pipeline_pyspark.py` is still available with `--combined`
  (written to `out/scripts/`). Scripts used to be written directly into `out/`.
- Simple statements stay Spark SQL. Complex ones (CTEs, subqueries, window functions, set operations, PIVOT, several joins) become
  DataFrame steps mixed with Spark SQL; a step the builder cannot express falls back to Spark SQL. `--mode auto|sql|mixed`.
- A plain `CREATE TABLE` is emitted as Iceberg DDL instead of being skipped.

### Added
- `reports/validate.json`, generated for you: runs your original SELECTs on the source (nothing is created there) and compares row counts,
  non-null counts and column sums with the Iceberg tables. `sql2iceberg validate` gets a `spark` target, `"tables": "auto"` and
  reference `"queries"`.
- Python API: `pyspark_scripts()`, `pyspark_outputs()`, `write_output_folder()`.

### Fixed (conversions that did not run, or gave other results than Snowflake)
- `LISTAGG` / `ARRAY_AGG ... WITHIN GROUP` and `LATERAL FLATTEN` (value/index) are converted instead of "unsupported".
- `PERCENTILE_CONT` is now exact (it was the approximate `percentile_approx`); `DATEDIFF` counts calendar boundaries like Snowflake;
  `ARRAY_SIZE(variant:path)` no longer fails in Spark.
- Snowflake `TIMESTAMP_TZ` / `TIMESTAMP_LTZ` columns can be copied over JDBC.

Known: in my test a windowed `AVG` came back from Snowflake with 5 decimals and from Spark with 6 (totals 15229.52982 and 15229.529997). `FLATTEN` supports value and index only and assumes JSON
text columns. Validation compares counts and sums, not every row.

**Full changelog:** https://github.com/jey1987-cmd/sql2iceberg/blob/main/CHANGELOG.md
