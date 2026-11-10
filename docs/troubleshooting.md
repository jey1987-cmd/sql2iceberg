# Troubleshooting

Start with `sql2iceberg doctor`: it checks Java, Spark, packages and certificates. If you still need help, run
`sql2iceberg issue --open` to open a GitHub issue pre-filled with your environment. Add `--trace trace.jsonl` to the failing
command and attach the trace after reading it.

Every entry below happened while building this library.

## Contents

- [UnsupportedClassVersionError: class file version 61.0](#unsupportedclassversionerror)
- [Wrong Spark version is used (SPARK_HOME)](#wrong-spark-version-is-used)
- [certificate_unknown / SSL errors](#certificate_unknown--ssl-errors)
- [Maven downloads fail (`--packages` cannot resolve)](#maven-downloads-fail)
- [Java 17 module errors (`InaccessibleObjectException`)](#java-17-module-errors)
- [JDBC catalog driver not found](#jdbc-catalog-driver-not-found)
- [A Hadoop catalog on S3 corrupts or fails](#hadoop-catalog-on-s3)
- [Snowflake JDBC with key-pair login](#snowflake-jdbc-with-key-pair-login)
- [Snowflake table shows old data after new writes](#snowflake-table-shows-old-data)
- [Snowflake: external volume or integration errors](#snowflake-external-volume-and-integration)
- [Glue: `register_table` fails because the table exists](#glue-register_table-fails)
- [Athena: access denied or no output location](#athena-access-denied-or-no-output-location)
- [GCP: billing disabled, permission denied, `invalid_grant`](#gcp-billing-permissions-and-expired-logins)
- [BigQuery cannot read my S3 bucket](#bigquery-cannot-read-my-s3-bucket)
- [BigQuery statement fails with "Unexpected end of script" on Windows](#bq-on-windows-truncates-multi-line-sql)
- [Iceberg format version 3](#iceberg-format-version-3)
- [A script hangs after Spark finishes (Windows)](#a-script-hangs-after-spark-finishes)
- ["Failed to delete" temp-directory errors on exit (Windows)](#failed-to-delete-temp-directory-on-exit)
- [pytest: `No module named 'sf2iceberg'` after upgrading from the old name](#upgrading-from-sf2iceberg)
- [Converted SQL runs but gives different results](#converted-sql-gives-different-results)
- [Frequently seen harmless messages](#harmless-messages)

---

### UnsupportedClassVersionError

```
java.lang.UnsupportedClassVersionError: org/apache/spark/launcher/Main ... class file version 61.0 ... up to 52.0
```
Spark started with Java 8 although `java -version` shows 17. Spark launches the JVM from **`JAVA_HOME`**, not from `PATH`.
`sql2iceberg doctor` prints both. Fix: point `JAVA_HOME` at a JDK 17 (for example `C:\Program Files\Eclipse Adoptium\jdk-17...`)
in the shell that runs Spark.

### Wrong Spark version is used

A global `SPARK_HOME` (for example an old Spark 3.0.3) overrides the Spark that comes with `pip install pyspark`, which
produces odd launcher or class errors. Unset `SPARK_HOME` or point it at `<venv>/Lib/site-packages/pyspark` (Windows) or
`<venv>/lib/pythonX.Y/site-packages/pyspark` (Linux/macOS). Also set `PYSPARK_PYTHON` to the venv's Python.

### certificate_unknown / SSL errors

Symptoms:
- Python or the AWS CLI: `SSL validation failed ... CERTIFICATE_VERIFY_FAILED ... unable to get local issuer certificate`
- Spark/Java: `SSLHandshakeException: (certificate_unknown) No trusted certificate found` (for example from the Snowflake JDBC driver)
- `bq` / `gcloud`: `Could not connect with BigQuery server due to: SSLCertVerificationError`

Cause: a proxy re-signs HTTPS traffic with a corporate certificate that your tools do not trust. Python, the AWS CLI and
gcloud use a PEM bundle; **Java uses its own truststore**, so it needs a separate fix.

```bash
# Python / boto3 / AWS CLI
export AWS_CA_BUNDLE=/path/to/ca-bundle.pem
export REQUESTS_CA_BUNDLE=/path/to/ca-bundle.pem
# gcloud / bq
export CLOUDSDK_CORE_CUSTOM_CA_CERTS_FILE=/path/to/ca-bundle.pem
# Java: copy the JDK truststore, import each certificate of the bundle, and pass it to Spark
cp "$JAVA_HOME/lib/security/cacerts" truststore.jks
keytool -importcert -noprompt -trustcacerts -alias corp1 -file corp1.pem -keystore truststore.jks -storepass changeit
export JDK_JAVA_OPTIONS="-Djavax.net.ssl.trustStore=/path/to/truststore.jks -Djavax.net.ssl.trustStorePassword=changeit"
```
`keytool` imports one certificate per call: split a multi-certificate bundle first. Keep the truststore out of the repository.

### Maven downloads fail

`spark.jars.packages` / `--packages` cannot reach Maven Central behind the same proxy (the JVM does not trust it), or you are
offline. Download each jar once (use `curl --cacert ca-bundle.pem`), check its `.sha1`, and use local files:
`MigrationConfig(jars=[...], iceberg_package="", jdbc_package="")`.

### Java 17 module errors

```
java.lang.reflect.InaccessibleObjectException: Unable to make field ... module java.base does not "opens java.nio"
```
Spark 4 launched through `spark-submit` adds the needed `--add-opens` flags. When you start PySpark from a plain Python
process (for example `python pipeline_pyspark.py`), set them yourself. The set that worked on JDK 17.0.21 with Spark 4.1.4
and the Snowflake JDBC driver:

```
JDK_JAVA_OPTIONS=-Djava.net.preferIPv4Stack=true --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED --add-opens=java.base/java.lang.reflect=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.util.concurrent=ALL-UNNAMED --add-opens=java.base/java.util.concurrent.atomic=ALL-UNNAMED --add-opens=java.base/jdk.internal.ref=ALL-UNNAMED --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/sun.nio.cs=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED --add-opens=java.base/sun.util.calendar=ALL-UNNAMED
```

### JDBC catalog driver not found

`java.sql.SQLException: No suitable driver` for `jdbc:sqlite:...`. The driver must be visible to the system class loader:
add the jar to `spark.driver.extraClassPath` as well as `spark.jars` (or use `--jars` with `spark-submit`).

### Hadoop catalog on S3

A Hadoop catalog commits by renaming files, which S3 cannot do atomically. Use `catalog_type="jdbc"` (a SQLite file, or any
JDBC database) or `glue` for S3 and GCS warehouses: `--catalog-type jdbc --catalog-uri jdbc:sqlite:catalog.db`. Keep the
SQLite file: it is the table list. Losing it does not lose the data, but you must re-register tables from their metadata files.

### Snowflake JDBC with key-pair login

Set `SNOWFLAKE_PRIVATE_KEY_PATH` to an **unencrypted PKCS#8** `.p8` file. The generated job passes it as the JDBC property
`private_key_file`. Include `warehouse` and `role` in the JDBC URL:
`jdbc:snowflake://<account>.snowflakecomputing.com/?warehouse=WH&role=ROLE`. `OCSPValidationError ... FAIL_OPEN` lines in
the output are informational.

### Snowflake table shows old data

Object-store Iceberg tables point at **one metadata file**. After new writes, point them at the new file:
`ALTER ICEBERG TABLE db.schema.t REFRESH 'path/relative/to/volume/new.metadata.json'`, or re-run `sql2iceberg publish`
with the new metadata location. Glue mode (`mode="glue"`) follows the catalog instead.

### Snowflake external volume and integration

- `Failed to access the REMOTE_FILE`/`access denied`: the IAM role in the volume must trust Snowflake's IAM user and external ID
  (`DESC EXTERNAL VOLUME <name>` shows both) and allow `s3:GetObject`, `s3:GetObjectVersion`, `s3:ListBucket` on the prefix.
- `insufficient privileges`: creating volumes and integrations needs `CREATE EXTERNAL VOLUME` and `CREATE INTEGRATION` (often
  ACCOUNTADMIN). If an admin already created them, set `create_volume=False` and `create_integration=False`.
- `METADATA_FILE_PATH` is **relative to the volume's base URL**; the publisher computes it and errors if the file is outside.

### Glue register_table fails

`register_table` fails if the table already exists in Glue. Drop the table first
(`aws glue delete-table --database-name D --name T`) or use `GlueTarget(already_in_glue=True)` to verify only. The
`CatalogNotFoundException: The catalog <db> not found` warning printed by Iceberg during registration is harmless.

### Athena access denied or no output location

Athena needs `athena_output="s3://bucket/prefix/"` that your identity can write to, plus `glue:GetTable`/`GetDatabase` and
`s3:GetObject` on the table location. Results are billed per data scanned.

### GCP billing, permissions and expired logins

- `billingEnabled: false` or *Billing must be enabled*: link an **open** billing account (`gcloud billing accounts list`,
  `gcloud billing projects link PROJECT --billing-account=ID`). Buckets and BigQuery connections need billing.
- `does not have storage.buckets.list access`: you are logged in as a different account from the project's owner.
  `gcloud auth list`, then `gcloud config set account ...` or log in again.
- `invalid_grant`: the login expired. Run `gcloud auth login` (and `gcloud auth application-default login` for the Python client).
- The Spark job passes a short-lived token through `GCS_OAUTH_TOKEN` and `GCS_OAUTH_TOKEN_EXPIRES_AT_MS`; it lasts about an hour.
  `export GCS_OAUTH_TOKEN="$(gcloud auth print-access-token)"` right before the run.
- After creating the BigQuery connection, grant its service account `roles/storage.objectViewer` on the bucket.

### BigQuery cannot read my S3 bucket

BigLake Iceberg tables read from Cloud Storage. Iceberg metadata stores absolute `s3://` paths, so copying files to GCS does
not work. Re-run the migration with a `gs://` warehouse (a second copy) or use BigQuery Omni with a bucket in a supported AWS
region.

### bq on Windows truncates multi-line SQL

`bq.cmd` cuts an argument at the first newline: `Syntax error: Unexpected end of script at [1:81]`. Collapse whitespace to
one line before passing SQL to the CLI, or use the Python client (`pip install "sql2iceberg[gcp]"`).

### Iceberg format version 3

Format-version 2 was tested on Snowflake, Athena and BigQuery. Version 3 (`--iceberg-version 3`) was verified once: a
Spark-written v3 table on S3 was read correctly by a Snowflake external Iceberg table (counts and sums matched the managed
table). Athena and BigQuery on v3 are untested, and the publishers warn when they see a v3 table. Use 2 if a platform rejects it.

### Scripts fail right at start with a Py4JJavaError (Spark version and Iceberg jar do not match)

The Iceberg runtime jar must match your Spark version. The generated scripts use the jar for Spark 4.1. If `pip` installed a newer
PySpark (for example 4.3), Spark cannot load it. Check with `pip show pyspark`, then `pip install "pyspark>=4.0,<4.2"`. From 0.2.1
`pip install "sql2iceberg[spark]"` installs a matching PySpark. For another Spark version, generate the scripts with the matching
Iceberg package or jar (`--jars` with `--no-maven`, see [environments.md](environments.md)).

### UNRECOGNIZED_SQL_TYPE when copying a Snowflake table

`[UNRECOGNIZED_SQL_TYPE] ... TIMESTAMPTZ`: Spark's JDBC reader does not know Snowflake's `TIMESTAMP_TZ` / `TIMESTAMP_LTZ`. Scripts generated by 0.2.0 or later read those columns as UTC `TIMESTAMP_NTZ` automatically. If you see this error, regenerate the scripts with a current version.

### A generated script fails after earlier ones worked

`run_all.py` stops at the first failing script and names it. Scripts before it already finished; fix the problem and run `python scripts/<name>_pyspark.py` for the failing one, then `python run_all.py` again, or run the remaining scripts by name: `python run_all.py NAME_pyspark.py ...`.

### A script hangs after Spark finishes

On Windows, Spark's child processes can keep stdout/stderr pipes open, so `subprocess.run(capture_output=True)` never returns.
Redirect to files instead of pipes.

### Failed to delete temp directory on exit

`ShutdownHookManager: Exception while deleting Spark temp dir ... Failed to delete ... .jar` on Windows is a file-lock race at
exit. The job has already finished; ignore it or delete the folder later.

### Upgrading from sf2iceberg

The project was renamed. `pip uninstall sf2iceberg` before `pip install sql2iceberg`; a stale pytest entry point causes
`ModuleNotFoundError: No module named 'sf2iceberg'`. Imports are now `import sql2iceberg`; the CLI is `sql2iceberg`; the test switch is `SQL2ICE_ICEBERG=1`.

### Converted SQL gives different results

The converter marks statements `review` or `unsupported` and explains why (see [known-issues.md](known-issues.md)): division by
zero, `DATEADD` on timestamps, `REGEXP_LIKE` anchoring, VARIANT paths, `TO_DATE` formats. Always run `sql2iceberg validate`
and, for a single statement, `sql2iceberg verify <case folder>` against exported results.

### Harmless messages

- `WARN Spark3Util: Failed to load catalog: <name>`: Iceberg probes whether a name is a catalog.
- `NativeCodeLoader ... using builtin-java classes`, `Using Spark's default log4j profile`: normal Spark start-up.
- `OCSPValidationError` with `ocspMode: FAIL_OPEN` from the Snowflake driver: informational.
- `RequestsDependencyWarning` from the `bq` CLI: its bundled libraries are old; the commands still work.
