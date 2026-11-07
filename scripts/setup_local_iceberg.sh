#!/usr/bin/env bash
# Local Iceberg "database" on your machine: PySpark + an Iceberg Hadoop catalog in ./warehouse.
# Needs Python 3.10+, Java 11/17 and internet access (Spark downloads the Iceberg jar from Maven Central on first run).
set -euo pipefail
python3 -m venv .venv
. .venv/bin/activate
pip install -e ".[spark,pytest,dev]"
python scripts/make_seed.py seed
export SF2ICE_ICEBERG=1
# Iceberg runtime for Spark 3.5. Override ICEBERG_PKG with a newer release if the v3 check below fails.
export ICEBERG_PKG="${ICEBERG_PKG:-org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.9.2}"
python - <<'PY'
from sf2iceberg.verify import make_spark
s = make_spark(iceberg=True, warehouse="./warehouse")
s.sql("DROP TABLE IF EXISTS v3check")
try:
    s.sql("CREATE TABLE v3check (id INT) USING iceberg TBLPROPERTIES ('format-version'='3')")
    print("format-version 3 table created: OK")
except Exception as e:
    print("format-version 3 NOT accepted by this Iceberg release:", str(e).splitlines()[0][:200])
PY
echo "Next: sf2iceberg generate examples/corpus/pipelines --ddl examples/corpus/ddl --out out"
