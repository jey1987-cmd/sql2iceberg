# Known issues behind the checks

Observed with SQLGlot 30.30.0 and PySpark 3.5.6 on made-up queries (no Snowflake account used).

Unsupported (output fails in Spark): SEQ4/GENERATOR, FLATTEN that uses KEY/PATH/SEQ/THIS, WITHIN GROUP with mixed ASC/DESC keys.
SQLGlot does not raise for these even with unsupported_level=RAISE.

Rewritten by sql2iceberg since 0.2.0 (SQLGlot's direct translation failed in Spark or gave other results):
`LISTAGG`/`ARRAY_AGG ... WITHIN GROUP` become a sorted `collect_list`/`collect_set` plus `array_join`; `LATERAL FLATTEN` (VALUE/INDEX only)
becomes `LATERAL VIEW [POS]EXPLODE(from_json(col, 'array<string>'))` and is flagged `FLATTEN_JSON` (assumes JSON text, as VARIANT/ARRAY
columns arrive over JDBC); `PERCENTILE_CONT` becomes the exact `percentile` (SQLGlot gave `percentile_approx`); `DATEDIFF(day|month|
quarter|year, ...)` counts calendar boundaries like Snowflake (Spark counts elapsed units on timestamps); `ARRAY_SIZE(variant:path)`
parses the JSON first. Checked against a live Snowflake account on 100 table-level checks; see CHANGELOG.

Known remaining difference: in my test, a windowed `AVG` over a `NUMBER(p,2)` column came back from Snowflake with 5 decimals and from Spark with 6
(the two column totals were 15229.52982 and 15229.529997, a difference around the 8th significant digit). Other engine versions may differ.

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
