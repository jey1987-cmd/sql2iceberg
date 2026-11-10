import pytest

from sql2iceberg import convert_statement, detect_sql_text
from sql2iceberg.connectors import REGISTRY, get_connector
from sql2iceberg.platforms import detect_platform


class FakeCursor:
    def __init__(self, rows):
        self.rows, self.sql = rows, None

    def execute(self, sql, params=()):
        self.sql = (sql, params)

    def fetchall(self):
        return self.rows

    def close(self):
        pass


class FakeConn:
    def __init__(self, rows):
        self.rows, self.closed = rows, False

    def cursor(self):
        return FakeCursor(self.rows)

    def close(self):
        self.closed = True


@pytest.mark.parametrize("name", sorted(REGISTRY))
def test_list_tables_groups_columns(name):
    c = get_connector(name)
    c._conn = FakeConn([("S", "T", "ID", "NUMBER", "NO"), ("S", "T", "NAME", "TEXT", "YES")])
    (t,) = c.list_tables("s")
    assert t.qualified == "S.T" and [x.name for x in t.columns] == ["ID", "NAME"]
    assert t.columns[0].nullable is False and t.columns[1].nullable is True


@pytest.mark.parametrize("name", sorted(REGISTRY))
def test_row_count_and_missing_env(name, monkeypatch):
    c = get_connector(name)
    c._conn = FakeConn([(42,)])
    assert c.row_count("s.t") == 42
    for var in c.required_env:
        monkeypatch.delenv(var, raising=False)
    with pytest.raises(RuntimeError, match="environment variables"):
        get_connector(name).query("select 1")


def test_only_snowflake_is_marked_tested():
    assert [n for n, c in REGISTRY.items() if c.tested] == ["snowflake"]


def test_unknown_platform():
    with pytest.raises(ValueError):
        get_connector("db2")


def test_jdbc_urls_have_no_credentials(monkeypatch):
    for k, v in {"SNOWFLAKE_ACCOUNT": "a", "PGHOST": "h", "PGDATABASE": "d", "MYSQL_HOST": "h",
                 "MYSQL_DATABASE": "d", "MSSQL_SERVER": "h", "MSSQL_DATABASE": "d", "ORACLE_DSN": "h:1/x"}.items():
        monkeypatch.setenv(k, v)
    for c in REGISTRY.values():
        url = c().jdbc_url()
        assert url.startswith("jdbc:") and "password" not in url.lower()


def test_detect_platform_from_imports(tmp_path):
    (tmp_path / "a.py").write_text("import psycopg2\nconn = psycopg2.connect()\n")
    r = detect_platform(tmp_path, env={})
    assert r["platform"] == "postgres"


def test_detect_platform_tie_returns_none(tmp_path):
    assert detect_platform(tmp_path, env={})["platform"] is None


def test_detect_platform_env_hint(tmp_path):
    assert detect_platform(tmp_path, env={"SNOWFLAKE_ACCOUNT": "x"})["platform"] == "snowflake"


def test_dialect_aware_detect_and_convert():
    (s,) = detect_sql_text("SELECT TOP 5 * FROM t", dialect="tsql")
    r = convert_statement(s.sql, dialect="tsql")
    assert r.spark_sql and "LIMIT 5" in r.spark_sql.upper()
    r = convert_statement("SELECT NVL(a, 0) FROM dual", dialect="oracle")
    assert r.status != "unparsed"
