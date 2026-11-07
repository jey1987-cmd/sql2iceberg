# sf2iceberg (working name)

Find Snowflake SQL in your code, convert it for Spark and Apache Iceberg, and check the result under pytest.

**Status: early alpha.** It converts with [SQLGlot](https://github.com/tobymao/sqlglot) and then adds checks SQLGlot does not do:
it flags output that is known not to run in Spark and conversions that can silently change results.
It does not yet compare results against Snowflake. See `docs/known-issues.md` and `docs/roadmap.md`.

## Install

    pip install sf2iceberg              # detect and convert (needs only sqlglot)
    pip install "sf2iceberg[pytest]"    # adds pytest plugin fixtures and local Spark

## Use

    sf2iceberg detect examples/            # list SQL found in .sql, .py and .ipynb files
    sf2iceberg convert examples/ --out out # write report.json and report.md
    sf2iceberg convert examples/ --fail-on-unsupported   # non-zero exit for CI

Each statement ends up as one of:

| status | meaning |
|---|---|
| converted | no known problem found (still verify it) |
| review | converted, but behavior may differ from Snowflake; the report says why |
| unsupported | output is known not to run in Spark; the report says what to do |
| unparsed / dynamic | could not parse, or built from variables (not converted) |

## Verify one case against Snowflake's results (offline)

A case is a folder with `case.json`, seed CSV files and an `expected.csv` that you export from your own Snowflake
account (run the original query on the same seed data). `verify` converts the SQL, runs it in local Spark on the
seed data, and compares rows (order ignored by default, float tolerance, types must match):

    sf2iceberg verify examples/cases/qualify            # passes
    sf2iceberg verify examples/cases/dateadd_timestamp  # fails: Spark returns a DATE, expected a timestamp
    sf2iceberg verify examples/cases/qualify --iceberg  # same, on an Iceberg Hadoop catalog (needs the jar)

Exit code is 0 only on a pass, so it works in CI. Needs `pip install "sf2iceberg[spark]"`. The `expected.csv` values in
`examples/cases/` were worked out by hand (the DATEADD one follows Snowflake's documented behavior): replace them with
real exports from your account before you rely on them.

## Map, generate, compare

    sf2iceberg map examples/corpus --out out            # tables each statement reads and writes (lineage.json)
    sf2iceberg generate examples/corpus/pipelines --ddl examples/corpus/ddl --out out
        # iceberg_spark.sql (format-version 3), snowflake_managed_iceberg.sql, pipeline_pyspark.py, warnings.txt
    sf2iceberg sql --counts orders customers            # SQL to run in Snowflake for row counts
    sf2iceberg compare --snowflake-counts sf.csv --iceberg-counts ice.csv --snowflake-credits 1.2 --credit-price 3

`examples/corpus/` is a made-up set of 45 Snowflake statements (windows, dates, VARIANT, regex, MERGE, PIVOT...).
`python scripts/run_corpus_smoke.py` runs the converted SELECTs in local Spark. See `docs/iceberg-setup.md`.
Snowflake DDL syntax follows Snowflake's docs but has not been run against a live account.

## Test your conversions with pytest

The plugin loads automatically. Fixtures: `sf2i_spark` (local Spark; set `SF2ICE_ICEBERG=1` for an Iceberg Hadoop catalog),
`sf2i_convert`. Tests marked `live_snowflake` are skipped unless you pass `--live-snowflake`, because they would spend credits.

    pytest -m spark

## What it does not do (yet)

- Query Snowflake for you: counts, expected results and credits come from CSVs/numbers you export yourself (a live `--live-snowflake` mode is planned).
- Rewrite unsupported constructs (it flags them).
- Convert stored procedures, tasks or streams.
- Iceberg-specific checks (MERGE, CTAS on Iceberg) are written but not yet run in CI.

## Safety

- Everything in `examples/` and `tests/` is made-up SQL and data.
- Credentials, when live mode exists, will be read from environment variables only.
- Nothing is sent to any model or service by default.

## License

MIT
