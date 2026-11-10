# Known issues behind the checks

Observed with SQLGlot 30.30.0 and PySpark 3.5.6 on made-up queries (no Snowflake account used).

Unsupported (output fails in Spark 3.5): FLATTEN (EXPLODE(input => ...)), ARRAY_AGG/LISTAGG ... WITHIN GROUP,
SEQ4/GENERATOR. SQLGlot does not raise for these even with unsupported_level=RAISE.

Review (runs, but may differ from Snowflake; confirm on your own Snowflake account):
DATEADD on timestamps returns a DATE; division by zero is NULL in Spark 3.5 and an error in Snowflake;
TO_DATE format argument dropped; REGEXP_LIKE whole-string vs RLIKE partial match; VARIANT path access assumes a JSON
string column; OBJECT_CONSTRUCT becomes STRUCT.

Not yet tested: Iceberg-specific behavior (MERGE INTO, CREATE OR REPLACE TABLE AS SELECT on Iceberg).
Run `SQL2ICE_ICEBERG=1 pytest -m spark` with network access to Maven Central.

## Found while building the synthetic corpus (Spark 3.5.6)

- `TO_VARCHAR(ts, 'YYYY-MM-DD')` becomes `CAST(ts AS STRING)`: the format is dropped (SQLGlot only logs a warning). Now flagged DROPPED.
- `MONTHNAME(x)` is emitted unchanged and Spark has no such function. Now flagged unsupported.
- `payload:user.id::NUMBER` can come out as `GET_PATH(...)`, which Spark lacks. Now flagged unsupported.
