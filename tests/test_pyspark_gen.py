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
    text = (out / "scripts" / "q_pyspark.py").read_text()
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
    text = (tmp_path / "o" / "scripts" / "q_pyspark.py").read_text()
    assert "GCSFileIO" in text and "os.environ['GCS_OAUTH_TOKEN']" in text


# ---- one script per SQL file; Spark SQL for simple statements, DataFrame API mixed in for complex ones ----
from sql2iceberg.detect import detect_path
from sql2iceberg.pyspark_gen import pyspark_scripts

SIMPLE = "CREATE OR REPLACE TABLE s AS SELECT status, COUNT(*) AS n FROM orders GROUP BY status;"
COMPLEX = """
CREATE OR REPLACE TABLE c AS
WITH big AS (SELECT customer_id, SUM(amount) AS spend FROM orders GROUP BY customer_id HAVING SUM(amount) > 10)
SELECT cu.name, b.spend, RANK() OVER (ORDER BY b.spend DESC) AS rnk
FROM customers cu JOIN big b ON b.customer_id = cu.customer_id;
"""
CFG = {"jdbc_url": "jdbc:snowflake://x.snowflakecomputing.com/"}


def _scripts(tmp_path, **files):
    for name, text in files.items():
        (tmp_path / name).write_text(text)
    return pyspark_scripts(detect_path(tmp_path), MigrationConfig(**CFG))


def test_one_script_per_sql_file_each_with_only_its_sources(tmp_path):
    out = _scripts(tmp_path, **{"a.sql": SIMPLE, "b.sql": COMPLEX})
    assert sorted(out) == ["a_pyspark.py", "b_pyspark.py"]
    for script, _ in out.values():
        compile(script, "x.py", "exec")
    assert "'orders'" in out["a_pyspark.py"][0] and "'customers'" not in out["a_pyspark.py"][0]
    assert "'customers'" in out["b_pyspark.py"][0] and "'orders'" in out["b_pyspark.py"][0]


def test_simple_statement_stays_spark_sql(tmp_path):
    script, report = _scripts(tmp_path, **{"a.sql": SIMPLE})["a_pyspark.py"]
    assert report[0]["strategy"] == "spark_sql" and 'spark.sql("""CREATE OR REPLACE TABLE lake.db.s USING iceberg' in script
    assert "functions as F" not in script


def test_complex_statement_mixes_dataframe_api_and_writes_with_writeto(tmp_path):
    script, report = _scripts(tmp_path, **{"b.sql": COMPLEX})["b_pyspark.py"]
    assert report[0]["strategy"] == "mixed" and report[0]["df_steps"] >= 2
    assert "df_big = (" in script and ".groupBy(" in script and ".join(df_big.alias('b')" in script
    assert "result.writeTo('lake.db.c')" in script and "functions as F" in script
    assert "CTE" in report[0]["note"] and "window function" in report[0]["note"]


def test_mode_sql_forces_spark_sql_and_mode_mixed_forces_dataframes(tmp_path):
    (tmp_path / "a.sql").write_text(SIMPLE)
    stmts = detect_path(tmp_path)
    cfg = MigrationConfig(**CFG)
    _, rep = pyspark_scripts(stmts, cfg, mode="sql")["a_pyspark.py"]
    assert rep[0]["strategy"] == "spark_sql"
    script, rep = pyspark_scripts(stmts, cfg, mode="mixed")["a_pyspark.py"]
    assert rep[0]["strategy"] == "mixed" and ".groupBy(" in script
    with pytest.raises(ValueError):
        pyspark_scripts(stmts, cfg, mode="nope")


def test_step_the_builder_cannot_express_falls_back_to_spark_sql_for_that_step_only(tmp_path):
    sql = """CREATE TABLE r AS WITH m AS (SELECT status, SUM(amount) AS s, SUM(SUM(amount)) OVER (ORDER BY status) AS run
    FROM orders GROUP BY status) SELECT status, s, run FROM m WHERE s > 0;"""
    script, report = _scripts(tmp_path, **{"c.sql": sql})["c_pyspark.py"]
    assert report[0]["df_steps"] == 1 and report[0]["sql_steps"] == 1
    assert "# CTE m: Spark SQL" in script and "df_m.createOrReplaceTempView('m')" in script
    assert "# final SELECT: DataFrame API" in script


def test_recursive_cte_stays_spark_sql(tmp_path):
    sql = "CREATE TABLE cal AS WITH RECURSIVE c (d) AS (SELECT 1 UNION ALL SELECT d + 1 FROM c WHERE d < 5) SELECT d FROM c;"
    _script, report = _scripts(tmp_path, **{"r.sql": sql})["r_pyspark.py"]
    assert report[0]["strategy"] == "spark_sql" and "recursive CTE" in report[0]["note"]


def test_previously_unsupported_statements_are_now_generated(tmp_path):
    sql = "CREATE TABLE t AS SELECT f.value::string AS tag, COUNT(*) AS n FROM customers c, LATERAL FLATTEN(input => c.tags) f GROUP BY f.value::string;"
    script, report = _scripts(tmp_path, **{"f.sql": sql})["f_pyspark.py"]
    assert report[0]["status"] == "review" and "NOT GENERATED" not in script
    assert "F.explode(" in script and "writeTo('lake.db.t')" in script


def test_insert_select_uses_append_helper_and_create_table_becomes_ddl(tmp_path):
    sql = """CREATE TABLE tgt (a NUMBER, b VARCHAR);
    INSERT INTO tgt (a, b) WITH x AS (SELECT id FROM src) SELECT id, 'k' FROM x;"""
    script, _ = _scripts(tmp_path, **{"i.sql": sql})["i_pyspark.py"]
    assert 'spark.sql("""CREATE TABLE lake.db.tgt (a DECIMAL(38, 0), b STRING) USING iceberg' in script
    assert "def append_df(" in script and "append_df(result, 'lake.db.tgt', ['a', 'b'])" in script
    compile(script, "i.py", "exec")


def test_script_header_names_the_scripts_it_depends_on(tmp_path):
    out = _scripts(tmp_path, **{"a.sql": "CREATE TABLE mid AS SELECT x FROM base;", "b.sql": "CREATE TABLE fin AS SELECT x FROM mid;"})
    assert "a_pyspark.py (mid)" in out["b_pyspark.py"][0] and "'mid'" not in out["b_pyspark.py"][0].split("curation")[0].split("SOURCES =")[1].split("spark =")[0]


def test_cli_writes_one_script_per_file_and_combined_on_request(tmp_path):
    from sql2iceberg.cli import main

    (tmp_path / "one.sql").write_text(SIMPLE)
    (tmp_path / "two.sql").write_text(COMPLEX)
    out = tmp_path / "out"
    args = ["pyspark", str(tmp_path), "--out", str(out), "--platform", "snowflake", "--jdbc-url", "jdbc:x"]
    assert main(args) == 0
    assert sorted(p.name for p in (out / "scripts").glob("*.py")) == ["one_pyspark.py", "two_pyspark.py"]
    assert main([*args, "--combined", "--mode", "sql"]) == 0
    assert (out / "scripts" / "pipeline_pyspark.py").exists()


def test_output_folder_is_organized_with_readme_run_order_and_reports(tmp_path):
    from sql2iceberg.cli import main

    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "a.sql").write_text("CREATE TABLE mid AS SELECT x FROM base;")
    (tmp_path / "src" / "b.sql").write_text("CREATE TABLE fin AS SELECT DATEADD(day, 1, x) AS y FROM mid;")
    out = tmp_path / "out"
    assert main(["pyspark", str(tmp_path / "src"), "--out", str(out), "--platform", "snowflake", "--jdbc-url", "jdbc:x"]) == 0
    assert {p.name for p in out.iterdir()} == {"README.md", "run_all.py", "scripts", "reports"}
    assert {p.name for p in (out / "reports").iterdir()} == {"conversion_report.md", "conversion_report.json", "validate.json"}
    readme = (out / "README.md").read_text()
    assert readme.index("a_pyspark.py") < readme.index("b_pyspark.py")           # run order: a creates what b reads
    assert "Converted, but please check" in readme and "DATEADD" in readme
    compile((out / "run_all.py").read_text(), "run_all.py", "exec")
    assert "ORDER = ['a_pyspark.py', 'b_pyspark.py']" in (out / "run_all.py").read_text()
    report = (out / "reports" / "conversion_report.md").read_text()
    assert "REVIEW DATEADD" in report and "Approach: Spark SQL" in report


def test_validate_config_uses_original_selects_and_qualifies_source_tables(tmp_path):
    from sql2iceberg.output import validate_config

    (tmp_path / "a.sql").write_text(
        "CREATE TABLE s AS WITH x AS (SELECT a FROM t) SELECT a FROM x;\n"
        "INSERT INTO d (a) SELECT a FROM t;\nINSERT INTO d (a) VALUES (1), (2);")
    cfg = MigrationConfig(jdbc_url="jdbc:x", source_schema="DB.SCH", warehouse="./wh", jars=["j.jar"])
    conf = validate_config(detect_path(tmp_path), cfg, "snowflake")
    assert conf["tables"] == "auto" and conf["reference"]["kind"] == "snowflake"
    q = conf["reference"]["queries"]
    assert q["s"] == ["WITH x AS (SELECT a FROM DB.SCH.t) SELECT a FROM x"]       # CTE name x is not qualified
    assert len(q["d"]) == 2 and q["d"][1].startswith("SELECT * FROM (VALUES")
    target = conf["targets"][0]
    assert target["kind"] == "spark" and target["tables"] == {"s": "lake.db.s", "d": "lake.db.d"} and target["jars"] == ["j.jar"]
    assert validate_config(detect_path(tmp_path), cfg, "snowflake") == conf
