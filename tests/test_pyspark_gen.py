import os
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest

from sql2iceberg.detect import detect_sql_text
from sql2iceberg.pyspark_gen import MigrationConfig, pyspark_pipeline, rewrite_for_iceberg

ROOT = Path(__file__).parent.parent
SQL = """
CREATE OR REPLACE TABLE curated AS
SELECT c.id, c.name, SUM(o.amt) AS total, IFF(SUM(o.amt) > 10, 'big', 'small') AS tier
FROM mydb.public.customers c JOIN orders o ON o.cust_id = c.id GROUP BY c.id, c.name;
"""


def _gen(**kw):
    cfg = MigrationConfig(jdbc_url="jdbc:snowflake://x.snowflakecomputing.com/", **kw)
    return pyspark_pipeline(detect_sql_text(SQL), cfg)


def test_script_compiles_and_has_no_secrets():
    script, report = _gen()
    compile(script, "pipeline_pyspark.py", "exec")
    assert "SNOWFLAKE_PASSWORD" in script and "os.environ" in script
    assert "'customers': ('mydb.public.customers'" in script and "'orders': ('orders'" in script
    assert report[0]["status"] in ("converted", "review")


def test_rewrite_points_tables_at_catalog_and_makes_ctas_iceberg():
    cfg = MigrationConfig()
    out = rewrite_for_iceberg("CREATE OR REPLACE TABLE curated AS WITH c AS (SELECT 1 AS x) SELECT x FROM c "
                              "JOIN db1.sch.t USING (x)", cfg)
    assert out.startswith("CREATE OR REPLACE TABLE lake.db.curated USING iceberg TBLPROPERTIES ('format-version'='2') AS")
    assert "lake.db.t" in out and "lake.db.c " not in out


def test_plain_create_table_is_skipped():
    assert rewrite_for_iceberg("CREATE TABLE t (a INT)", MigrationConfig()) is None


def test_catalog_types_and_validation():
    script, _ = _gen(catalog_type="glue", warehouse="s3://bkt/wh")
    assert "GlueCatalog" in script and "S3FileIO" in script
    with pytest.raises(ValueError):
        MigrationConfig(catalog_type="nope")


def test_unsupported_statement_is_commented_out():
    stmts = detect_sql_text("SELECT MONTHNAME(ts) FROM o")
    script, report = pyspark_pipeline(stmts, MigrationConfig(jdbc_url="jdbc:x"))
    assert "NOT GENERATED" in script and report[0]["status"] == "unsupported"


@pytest.mark.spark
def test_generated_script_runs_end_to_end(tmp_path):
    """SQLite over JDBC -> Iceberg (Hadoop catalog) -> curated CTAS, with the generated script as-is."""
    jars = [ROOT / "jars" / n for n in ("iceberg-spark-runtime-4.1_2.13-1.12.0.jar", "sqlite-jdbc-3.46.1.3.jar")]
    if not all(j.exists() for j in jars) or os.environ.get("SQL2ICE_ICEBERG") != "1":
        pytest.skip("needs SQL2ICE_ICEBERG=1 and the jars in ./jars")
    pytest.importorskip("pyspark")
    db = tmp_path / "src.db"
    con = sqlite3.connect(db)
    con.executescript("""
        CREATE TABLE customers (id INTEGER, name TEXT);
        CREATE TABLE orders (cust_id INTEGER, amt REAL);
        INSERT INTO customers VALUES (1,'a'),(2,'b');
        INSERT INTO orders VALUES (1,5),(1,10),(2,3);
    """)
    con.commit()
    con.close()
    cfg = MigrationConfig(jdbc_url=f"jdbc:sqlite:{db}", jdbc_driver="org.sqlite.JDBC", jdbc_package="",
                          iceberg_package="", jars=[str(j) for j in jars], warehouse=str(tmp_path / "wh"))
    script, _ = pyspark_pipeline(detect_sql_text(SQL.replace("mydb.public.", "")), cfg)
    script = script.replace("spark.stop()\n", "") + (
        "rows = spark.sql('SELECT id, total, tier FROM lake.db.curated ORDER BY id').collect()\n"
        "print('RESULT', [tuple(r) for r in rows])\n"
        "print('COUNTS', spark.table('lake.db.customers').count(), spark.table('lake.db.orders').count())\n"
        "spark.stop()\n"
    )
    f = tmp_path / "pipeline.py"
    f.write_text(script, encoding="utf-8")
    env = {**os.environ, "SNOWFLAKE_USER": "u", "SNOWFLAKE_PASSWORD": "p", "PYSPARK_PYTHON": sys.executable}
    env.pop("SPARK_HOME", None)  # a stale SPARK_HOME would override the pip-installed PySpark
    java = shutil.which("java")
    if java:  # JAVA_HOME may point at an old JDK while PATH has a newer one
        env["JAVA_HOME"] = str(Path(java).resolve().parent.parent)
    # files, not pipes: Spark's child processes can keep a pipe open after the script exits (Windows)
    with open(tmp_path / "out.txt", "w") as out, open(tmp_path / "err.txt", "w") as err:
        rc = subprocess.run([sys.executable, str(f)], stdout=out, stderr=err, env=env, timeout=300, check=False).returncode
    stdout = (tmp_path / "out.txt").read_text()
    assert rc == 0, (tmp_path / "err.txt").read_text()[-3000:]
    assert "RESULT [(1, 15.0, 'big'), (2, 3.0, 'small')]" in stdout, stdout[-2000:]
    assert "COUNTS 2 3" in stdout


def test_snowflake_script_supports_key_pair():
    script, _ = _gen()
    assert "SNOWFLAKE_PRIVATE_KEY_PATH" in script and "private_key_file" in script


def test_gcs_warehouse_uses_gcs_fileio_and_env_token():
    script, _ = _gen(warehouse="gs://bkt/wh", catalog_type="jdbc", catalog_uri="jdbc:sqlite:c.db",
                     gcs_project="p1", gcs_token_from_env=True)
    compile(script, "p.py", "exec")
    assert "GCSFileIO" in script and "os.environ['GCS_OAUTH_TOKEN']" in script
    assert "ya29" not in script


def test_cli_pyspark_accepts_documented_flags(tmp_path):
    """The README quick start command must keep working (jdbc catalog, S3, region, local jars)."""
    from sql2iceberg.cli import main

    sql = tmp_path / "q.sql"
    sql.write_text("CREATE OR REPLACE TABLE t AS SELECT a FROM src_tbl;")
    out = tmp_path / "out"
    rc = main(["pyspark", str(sql), "--out", str(out), "--platform", "snowflake",
               "--jdbc-url", "jdbc:snowflake://x.snowflakecomputing.com/", "--catalog-type", "jdbc",
               "--catalog-uri", "jdbc:sqlite:catalog.db", "--warehouse", "s3://bkt/wh", "--namespace", "live",
               "--iceberg-version", "2", "--region", "eu-west-1", "--jars", "a.jar,b.jar", "--no-maven"])
    assert rc == 0
    text = (out / "pipeline_pyspark.py").read_text()
    compile(text, "p.py", "exec")
    assert "JdbcCatalog" in text and "eu-west-1" in text and "a.jar,b.jar" in text and "jars.packages" not in text
    assert "lake.live.t" in text and "'src_tbl'" in text


def test_cli_pyspark_gcs_flags(tmp_path):
    from sql2iceberg.cli import main

    sql = tmp_path / "q.sql"
    sql.write_text("CREATE TABLE t AS SELECT a FROM s;")
    assert main(["pyspark", str(sql), "--out", str(tmp_path / "o"), "--platform", "snowflake", "--jdbc-url", "jdbc:x",
                 "--catalog-type", "jdbc", "--catalog-uri", "jdbc:sqlite:c.db", "--warehouse", "gs://b/wh",
                 "--gcs-project", "p1", "--gcs-token-from-env"]) == 0
    text = (tmp_path / "o" / "pipeline_pyspark.py").read_text()
    assert "GCSFileIO" in text and "os.environ['GCS_OAUTH_TOKEN']" in text
