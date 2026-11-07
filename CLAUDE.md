# sf2iceberg: notes for Claude Code

Open-source library (MIT, personal project, made-up SQL and data only). Detects Snowflake SQL in code, converts it to
Spark SQL with SQLGlot, flags what cannot be trusted, generates Iceberg DDL, and verifies against exported results.

- Layout: src/sf2iceberg/{detect,convert,lineage,generate,compare,verify,report,cli,pytest_plugin}.py; tests/; examples/corpus/.
- Run tests: `pytest -q` (Spark tests need PySpark and Java 17). Lint: `ruff check .`
- Current goal: local Spark 4 + Iceberg setup (user is on Windows, venv at .venv), then confirm `format-version=3` tables work.
- Unverified: which Iceberg runtime jar goes with Spark 4 (try iceberg-spark-runtime-4.0_2.13), Snowflake Iceberg type support, anything on a live Snowflake account.
- Rules: no real company SQL or data; do not push to GitHub or publish to PyPI without the user's explicit say-so; never put credentials in files; ask before anything that spends Snowflake credits.
- Convert checks come from observed Spark behavior (docs/known-issues.md). Re-check against Spark 4 (ANSI mode is on by default).
