# Roadmap (0.x)

0. Done: offline `verify` (seed CSV, run converted SQL in Spark, compare with exported expected CSV).
1. Iceberg-enabled execution tests (MERGE, CTAS) on a Hadoop catalog.
2. Seed synthetic data from DDL; run the original in the user's own Snowflake account and compare results (row counts, keys, nulls, aggregates, samples) behind `--live-snowflake`.
3. Rewrite rules for FLATTEN, WITHIN GROUP, GENERATOR, DATEADD on timestamps.
4. Sign-off record (per statement status, versions, file hashes, named reviewer).
5. Optional model-assisted translation behind the same verification gate (off by default).
