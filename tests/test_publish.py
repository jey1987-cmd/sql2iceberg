import pytest

from sql2iceberg.publish import (
    BigQueryPublisher,
    BigQueryTarget,
    GluePublisher,
    GlueTarget,
    IcebergTable,
    SnowflakePublisher,
    SnowflakeTarget,
    latest_local_metadata,
)
from sql2iceberg.publish.base import ident, lit

TB = [IcebergTable("orders", "db", "s3://bkt/wh/db/orders/metadata/00003-abc.metadata.json", 2)]
ARN = "arn:aws:iam::123456789012:role/r"


def sf(**kw):
    return SnowflakeTarget("DB", "SCH", "s3://bkt/wh/", aws_role_arn=ARN, **kw)


def test_snowflake_object_store_plan():
    res = SnowflakePublisher(sf()).plan(TB)
    text = "\n".join(a.text for a in res.actions)
    assert "STORAGE_BASE_URL = 's3://bkt/wh/'" in text and f"STORAGE_AWS_ROLE_ARN = '{ARN}'" in text
    assert "CATALOG_SOURCE = OBJECT_STORE" in text
    assert "METADATA_FILE_PATH = 'db/orders/metadata/00003-abc.metadata.json'" in text
    assert any(a.costs_money for a in res.actions) and all(not a.done for a in res.actions)


def test_snowflake_glue_plan_and_validation():
    t = sf(mode="glue", glue_catalog_id="123456789012", glue_region="us-east-2")
    text = "\n".join(a.text for a in SnowflakePublisher(t).plan(TB).actions)
    assert "CATALOG_SOURCE = GLUE" in text and "CATALOG_TABLE_NAME = 'orders'" in text
    with pytest.raises(ValueError):
        sf(mode="glue")  # missing glue ids
    with pytest.raises(ValueError):
        SnowflakeTarget("DB", "SCH", "ftp://x/")
    with pytest.raises(ValueError):
        SnowflakeTarget("DB; DROP", "SCH", "s3://b/", aws_role_arn=ARN)


def test_snowflake_gcs_volume_and_outside_base():
    t = SnowflakeTarget("DB", "SCH", "gs://bkt/wh")
    pub = SnowflakePublisher(t)
    assert "STORAGE_PROVIDER = 'GCS'" in pub._volume_sql() and "gcs://bkt/wh/" in pub._volume_sql()
    assert pub._relative_path("gs://bkt/wh/db/t/metadata/v1.metadata.json") == "db/t/metadata/v1.metadata.json"
    with pytest.raises(ValueError):
        pub._relative_path("gs://other/x.json")


def test_v3_warning():
    res = SnowflakePublisher(sf()).plan([IcebergTable("t", "db", TB[0].metadata_location, 3)])
    assert any("format-version 3" in w for w in res.warnings)


def test_apply_requires_confirm_and_stops_on_error():
    class Conn:
        def __init__(self):
            self.sql = []

        def query(self, s):
            self.sql.append(s)
            if "CATALOG INTEGRATION" in s:
                raise RuntimeError("no privilege")
            return []

    c = Conn()
    pub = SnowflakePublisher(sf(), connector=c)
    with pytest.raises(PermissionError):
        pub.apply(TB)
    assert c.sql == []
    res = pub.apply(TB, confirm=True)
    assert not res.ok and res.actions[1].error.startswith("RuntimeError") and len(res.actions) > len(c.sql)
    assert res.actions[0].done and not res.actions[2].done


def test_glue_register_plan_and_s3_requirement():
    res = GluePublisher(GlueTarget("db", athena_output="s3://bkt/athena/")).plan(TB)
    text = "\n".join(a.text for a in res.actions)
    assert "register_table(table => 'db.orders', metadata_file => 's3://bkt/wh/db/orders/metadata/00003-abc.metadata.json')" in text
    assert any("Athena" in a.description and a.costs_money for a in res.actions)
    with pytest.raises(ValueError):
        GluePublisher(GlueTarget("db")).plan([IcebergTable("t", "db", "/local/x.json")])
    already = GluePublisher(GlueTarget("db", already_in_glue=True)).plan(TB)
    assert [a.kind for a in already.actions] == ["api"]


def test_glue_verify_uses_boto_session():
    class Glue:
        def get_table(self, DatabaseName, Name):
            return {"Table": {"Parameters": {"table_type": "ICEBERG"}}}

    class Session:
        def client(self, name):
            return Glue()

    res = GluePublisher(GlueTarget("db", already_in_glue=True), session=Session()).apply(TB, confirm=True)
    assert res.ok


def test_bigquery_plan_requires_gcs():
    t = BigQueryTarget("my-proj", "ds", "my-proj.us.conn")
    gcs = [IcebergTable("orders", "db", "gs://bkt/wh/db/orders/metadata/v1.metadata.json")]
    text = "\n".join(a.text for a in BigQueryPublisher(t).plan(gcs).actions)
    assert "format = 'ICEBERG'" in text and "WITH CONNECTION `my-proj.us.conn`" in text
    with pytest.raises(ValueError):
        BigQueryPublisher(t).plan(TB)
    with pytest.raises(ValueError):
        BigQueryTarget("p", "ds", "bad")


def test_ident_and_lit():
    assert ident("ok_1$") == "ok_1$"
    for bad in ("", "1x", "a b", "a;b", "a.b"):
        with pytest.raises(ValueError):
            ident(bad)
    assert lit("it's") == "'it''s'"


def test_latest_local_metadata(tmp_path):
    m = tmp_path / "db" / "t" / "metadata"
    m.mkdir(parents=True)
    for n in ("v1.metadata.json", "v10.metadata.json", "v2.metadata.json", "version-hint.text", "snap-1.avro"):
        (m / n).write_text("{}")
    assert latest_local_metadata(tmp_path, "db", "t").name == "v10.metadata.json"
    with pytest.raises(FileNotFoundError):
        latest_local_metadata(tmp_path, "db", "nope")


def test_markdown_report_lists_actions():
    md = SnowflakePublisher(sf()).plan(TB).to_markdown()
    assert "## 1. Create external volume" in md and "```sql" in md


def test_cli_publish_prints_plan_and_does_not_apply(tmp_path, capsys):
    import json

    from sql2iceberg.cli import main

    cfg = {"target": "snowflake",
           "snowflake": {"database": "DB", "schema": "S", "base_url": "s3://bkt/wh/", "aws_role_arn": ARN},
           "tables": [{"name": "orders", "metadata_location": TB[0].metadata_location}]}
    f = tmp_path / "p.json"
    f.write_text(json.dumps(cfg))
    assert main(["publish", str(f)]) == 0
    out = capsys.readouterr().out
    assert "planned" in out and "applied" not in out


def test_reuse_existing_volume_and_integration():
    t = sf(create_volume=False, create_integration=False, create_schema=False)
    descs = [a.description for a in SnowflakePublisher(t).plan(TB).actions]
    assert descs == ["Expose orders", "Count rows (uses a warehouse)"]


def test_tables_from_jdbc_catalog(tmp_path):
    import sqlite3

    from sql2iceberg.publish import tables_from_jdbc_catalog

    f = tmp_path / "c.db"
    con = sqlite3.connect(f)
    con.execute("CREATE TABLE iceberg_tables (catalog_name, table_namespace, table_name, metadata_location, previous_metadata_location)")
    con.executemany("INSERT INTO iceberg_tables VALUES (?,?,?,?,NULL)",
                    [("lake", "live", "b", "s3://x/b.metadata.json"), ("lake", "live", "a", "s3://x/a.metadata.json"),
                     ("lake", "other", "z", "s3://x/z.metadata.json")])
    con.commit()
    con.close()
    got = tables_from_jdbc_catalog(f, "live")
    assert [(t.name, t.metadata_location) for t in got] == [("a", "s3://x/a.metadata.json"), ("b", "s3://x/b.metadata.json")]
