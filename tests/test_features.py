from pathlib import Path

from sql2iceberg.cli import main
from sql2iceberg.compare import compare_counts, cost_summary, count_sql, credits_sql, read_counts
from sql2iceberg.convert import convert_statement
from sql2iceberg.detect import detect_path
from sql2iceberg.generate import snowflake_iceberg_ddl, spark_iceberg_ddl
from sql2iceberg.lineage import map_statement, map_statements

CORPUS = Path(__file__).parent.parent / "examples" / "corpus"
DDL = (CORPUS / "ddl" / "schema.sql").read_text()


def test_lineage_ctas_and_join():
    m = map_statement("CREATE TABLE t AS SELECT a.x FROM a JOIN b ON a.id = b.id")
    assert m["outputs"] == ["t"] and m["inputs"] == ["a", "b"]


def test_lineage_cte_and_merge():
    m = map_statement("WITH c AS (SELECT * FROM src) SELECT * FROM c")
    assert m["inputs"] == ["src"]
    m = map_statement("MERGE INTO tgt t USING stg s ON t.id = s.id WHEN MATCHED THEN UPDATE SET t.v = s.v")
    assert m["outputs"] == ["tgt"] and "stg" in m["inputs"]


def test_lineage_corpus_sources():
    lin = map_statements(detect_path(CORPUS / "pipelines"))
    assert "staging_customers" in lin["source_tables"]
    assert "daily_sales" in lin["target_tables"]


def test_dropped_format_is_flagged():
    r = convert_statement("SELECT TO_VARCHAR(ts, 'YYYY-MM-DD') FROM o")
    assert r.status == "review" and any(f.code == "DROPPED" for f in r.findings)


def test_unknown_spark_functions_unsupported():
    assert convert_statement("SELECT MONTHNAME(ts) FROM o").status == "unsupported"


def test_spark_ddl_is_v3_and_maps_variant():
    ddl, warnings = spark_iceberg_ddl(DDL)
    assert "'format-version'='3'" in ddl and "USING iceberg" in ddl
    assert "attrs STRING" in ddl and any("VARIANT" in w for w in warnings)


def test_snowflake_ddl_syntax():
    ddl, _ = snowflake_iceberg_ddl(DDL, external_volume="my_vol")
    assert "CREATE ICEBERG TABLE IF NOT EXISTS orders" in ddl
    assert "CATALOG = 'SNOWFLAKE'" in ddl and "ICEBERG_VERSION = 3" in ddl and "EXTERNAL_VOLUME = 'my_vol'" in ddl


def test_compare_counts_and_cost(tmp_path):
    a, b = tmp_path / "a.csv", tmp_path / "b.csv"
    a.write_text("table_name,row_count\nDB.S.ORDERS,100\ncustomers,20\n")
    b.write_text("table_name,row_count\norders,100\ncustomers,19\n")
    res = {r["table"]: r["status"] for r in compare_counts(read_counts(a), read_counts(b))}
    assert res == {"customers": "mismatch", "orders": "match"}
    c = cost_summary(2.0, 3.0, 1800, 4.0)
    assert c["snowflake_cost"] == 6.0 and c["spark_cost"] == 2.0
    assert "COUNT(*)" in count_sql(["a", "b"]) and "warehouse_metering_history" in credits_sql("W", "s", "e")


def test_cli_map_generate(tmp_path, capsys):
    assert main(["map", str(CORPUS), "--out", str(tmp_path / "m")]) == 0
    assert (tmp_path / "m" / "lineage.json").exists()
    assert main(["generate", str(CORPUS / "pipelines"), "--ddl", str(CORPUS / "ddl"), "--out", str(tmp_path / "g")]) == 0
    assert (tmp_path / "g" / "pipeline_pyspark.py").exists()
    compile((tmp_path / "g" / "pipeline_pyspark.py").read_text(), "gen", "exec")
