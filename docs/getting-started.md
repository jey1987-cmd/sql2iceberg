# Getting started (no experience needed)

This guide takes you from "I have some SQL" to "I have PySpark scripts that write Iceberg tables, and I know they give the same
answers". Nothing here assumes you know Spark or Iceberg. Words in **bold** are explained in the [glossary](#glossary) at the end.

## What does this tool do?

You give it a folder with SQL files (written for Snowflake, Postgres, MySQL, SQL Server or Oracle). It gives you back, **for every SQL file,
one PySpark script**. When you run that script it:

1. copies the tables the SQL needs from your database into **Iceberg** tables,
2. does the same transformation your SQL did,
3. saves the result as a new Iceberg table.

You do not have to rewrite anything by hand. If the tool cannot convert something, it tells you exactly what and why.

## Before you start

You need:

| What | Why | How to check |
|---|---|---|
| Python 3.10 or newer | runs the tool | `python --version` |
| Java 17 | Spark runs on Java | `java -version` |
| A login to the database your SQL is written for | the scripts copy tables from it | you know your user, account and key or password |

Then install the tool (once):

```bash
pip install "sql2iceberg[spark,snowflake]"      # use [spark,postgres] / [spark,mysql] ... for other databases
sql2iceberg doctor                               # checks Java, Spark and packages and tells you what is missing
```

Behind a company network that inspects HTTPS traffic? If something fails with a certificate error, read
[troubleshooting](troubleshooting.md#certificate_unknown--ssl-errors) first.

## Step 1: put your SQL in a folder

One statement per file works best, for example `sqls/customer_summary.sql`:

```sql
CREATE OR REPLACE TABLE customer_summary AS
SELECT c.customer_id, c.name, COUNT(o.order_id) AS order_count, SUM(o.amount) AS total_spent
FROM customers c
LEFT JOIN orders o ON o.customer_id = c.customer_id
GROUP BY c.customer_id, c.name;
```

Use `CREATE TABLE ... AS SELECT` (the result becomes a table) or `INSERT INTO ... SELECT`. A file may hold several statements.

Write `CREATE OR REPLACE TABLE` (not plain `CREATE TABLE`) if you want to be able to run the generated script again: a plain `CREATE TABLE` fails the second time because the table already exists.

## Step 2: see what the tool finds

```bash
sql2iceberg detect sqls          # which database the SQL looks like, and the statements found
sql2iceberg map sqls             # which tables are read (sources) and which are created (results)
```

Tip: the tool guesses the database from the SQL, but many databases share syntax (a `::date` cast is valid in both Postgres and Snowflake). When you know which database your SQL is written for, add `--platform postgres` (or `snowflake`, `mysql`, `mssql`, `oracle`) to these commands.

## Step 3: generate the scripts

```bash
sql2iceberg pyspark sqls --out out --platform snowflake \
    --source-schema MY_DB.MY_SCHEMA \
    --jdbc-url "jdbc:snowflake://<account>.snowflakecomputing.com/?warehouse=<WAREHOUSE>&role=<ROLE>" \
    --warehouse ./warehouse
```

What the options mean:

| Option | Meaning |
|---|---|
| `sqls` | the folder with your SQL |
| `--out out` | where to put the result |
| `--platform snowflake` | the database your SQL is written for (`snowflake`, `postgres`, `mysql`, `mssql`, `oracle`) |
| `--source-schema` | where your tables live in that database, so `customers` can be found as `MY_DB.MY_SCHEMA.customers` |
| `--jdbc-url` | how Spark connects to the database (see [environments](environments.md)) |
| `--warehouse` | where the Iceberg tables are saved: a local folder to start with, or `s3://bucket/path` later |

The tool prints one line per script and then points you to `out/README.md`.

## Step 4: read the output folder

```
out/
  README.md                       start here: the scripts, the order to run them, and what to double-check
  run_all.py                      runs all scripts in the right order
  scripts/
    customer_summary_pyspark.py   one script per SQL file
  reports/
    conversion_report.md          every statement: how it was converted and what to check
    conversion_report.json        the same, for programs
    validate.json                 a ready-made check of the results (step 6)
```

Open `out/README.md`. It lists three things:

* **The scripts and their order.** If one SQL file reads a table that another file creates, the second script is listed after the first.
* **Needs your attention.** Statements that could not be converted. There should be none for common SQL; if there are, the
  message says what is not supported and what to do.
* **Converted, but please check.** Statements that were converted but where Spark behaves a little differently from your database
  (for example dividing by zero). Each has a one-line explanation.

### How each script does the work

The tool picks the clearer approach per statement, you do not have to:

* A **simple** query (one table or one join, no CTEs) is run as **Spark SQL**: your SQL, translated to Spark's dialect.
* A **complex** query (CTEs, subqueries, window functions, UNION, PIVOT, several joins) is built step by step: **each CTE is a
  PySpark DataFrame**, put together with DataFrame operations (`join`, `where`, `groupBy().agg()`, `select`, `union`, pivot, explode).
  A step that is clearer as SQL stays SQL. The result is identical; each step is named after your CTE so you can follow it.

Want something else? `--mode sql` uses Spark SQL everywhere, `--mode mixed` uses DataFrames wherever possible.

## Step 5: run

Set your database login in the environment (the exact names are at the top of every script):

```bash
export SNOWFLAKE_USER=me
export SNOWFLAKE_PRIVATE_KEY_PATH=/path/to/rsa_key.p8     # or SNOWFLAKE_PASSWORD=...
python out/run_all.py                                       # everything, in order
python out/scripts/customer_summary_pyspark.py              # or just one script
```

On Windows PowerShell use `$env:SNOWFLAKE_USER = "me"` instead of `export`.

Afterwards the Iceberg tables are in your warehouse folder (`warehouse/db/customer_summary/`).

## Step 6: check the answers match

A converted script that runs is not proof that it is right. The tool wrote a ready-made check for you,
`out/reports/validate.json`. It runs **your original SELECTs on the source database** (nothing is created or changed there) and compares,
for every result table: the number of rows, the number of non-empty values in each column, and the sum of each number column.

```bash
sql2iceberg validate out/reports/validate.json --out out/reports
```

Use the same login variables as in step 5 and run it from the same folder (so the warehouse path is found). It prints a table and
writes `out/reports/validation.md`. `Overall: PASS` means every check matched; any `NO` row names the table and the check that differs.

What this proves and what it does not: matching counts and sums make a wrong conversion very unlikely, but it does not compare every
row. For text or date results that you want to be sure about, also compare a few rows by eye. Always look at the statements listed
under "Converted, but please check" in `out/README.md`.

## When something goes wrong

| You see | Try |
|---|---|
| `JAVA_GATEWAY_EXITED` or `UnsupportedClassVersionError` | Java is the wrong version: use Java 17 ([troubleshooting](troubleshooting.md#java-17-module-errors)) |
| a certificate error (`PKIX`, `No trusted certificate found`) | your network inspects HTTPS: [troubleshooting](troubleshooting.md#certificate_unknown--ssl-errors) |
| `[UNRECOGNIZED_SQL_TYPE]` | a column type Spark cannot read; update to the latest version (Snowflake date-time with zone is handled automatically) |
| `error: ... not set` | a login variable is missing: the message names it |
| a statement under "Needs your attention" | read the message in `reports/conversion_report.md`; it says what to change |
| anything else | `sql2iceberg doctor`, then `sql2iceberg issue --title "short summary" --open` |

Run one script at a time if a run fails: scripts before the failing one already finished and do not need to run again.

## Glossary

* **SQL**: the query language of databases.
* **Spark / PySpark**: an engine that processes large tables; PySpark is Spark used from Python.
* **Spark SQL**: SQL run by Spark (a slightly different dialect from Snowflake or Postgres).
* **DataFrame**: a table inside PySpark that you transform with Python code.
* **CTE**: `WITH name AS (SELECT ...)`, a named step inside a query.
* **Apache Iceberg**: a table format for files in a data lake. Several tools (Spark, Snowflake, Athena, BigQuery) can read the same table.
* **Warehouse**: the folder or bucket where the Iceberg tables are stored.
* **JDBC**: how Spark connects to a database to copy its tables.
