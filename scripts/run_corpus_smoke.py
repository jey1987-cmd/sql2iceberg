"""Run every converted corpus statement in local Spark on the seed data; report what executes.
This only shows whether Spark accepts the SQL, not whether results match Snowflake."""
import subprocess
import sys
import tempfile
from pathlib import Path

from sf2iceberg.convert import convert_statement
from sf2iceberg.detect import detect_path
from sf2iceberg.verify import make_spark

seed = Path(tempfile.mkdtemp()); subprocess.run([sys.executable, "scripts/make_seed.py", str(seed)], check=True)
spark = make_spark()
for t, schema in {"customers": "customer_id INT, name STRING, country STRING, signup_ts TIMESTAMP",
                  "orders": "order_id INT, customer_id INT, order_ts TIMESTAMP, amount DOUBLE, status STRING, coupon STRING",
                  "order_items": "order_id INT, sku STRING, qty INT, price DOUBLE"}.items():
    spark.read.csv(str(seed / f"{t}.csv"), header=True, schema=schema).createOrReplaceTempView(t)
spark.createDataFrame([(1, 1, '{"user":{"id":7},"items":[{"sku":"A"}]}')], "event_id INT, customer_id INT, payload STRING"
                      ).createOrReplaceTempView("events")  # events.payload as a JSON string
spark.sql("SELECT *, array('a','b') AS tags FROM customers").createOrReplaceTempView("customers")
ok = bad = 0
for s in detect_path(sys.argv[1] if len(sys.argv) > 1 else "examples/corpus/pipelines"):
    r = convert_statement(s.sql)
    if r.status == "unsupported" or not r.spark_sql or not r.spark_sql.lstrip().upper().startswith("SELECT"):
        continue
    try:
        spark.sql(r.spark_sql).collect(); ok += 1
    except Exception as e:  # noqa: BLE001
        bad += 1; print("FAIL", r.status, s.sql[:70].replace("\n", " "), "->", str(e).splitlines()[0][:110])
print(f"select statements run: {ok} ok, {bad} failed")
