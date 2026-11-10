import json
import sqlite3

import pytest

from sql2iceberg import diagnostics
from sql2iceberg.cli import main
from sql2iceberg.tracing import Tracer, redact, set_tracer, span
from sql2iceberg.validate import ColumnSpec, Target, validate


def db(rows):
    con = sqlite3.connect(":memory:")
    con.execute("CREATE TABLE t (id INTEGER, amt REAL, name TEXT)")
    con.executemany("INSERT INTO t VALUES (?,?,?)", rows)
    return lambda sql: con.execute(sql).fetchall()


COLS = {"t": [ColumnSpec("id"), ColumnSpec("amt", numeric=True), ColumnSpec("name")]}
ROWS = [(1, 1.5, "a"), (2, 2.5, None), (3, None, "c")]


def tg(name, rows):
    return Target(name, db(rows), {"t": "t"}, cast="REAL")


def test_validate_passes_on_equal_data():
    rep = validate(COLS, tg("src", ROWS), [tg("a", ROWS), tg("b", ROWS)])
    assert rep.ok and "PASS" in rep.to_markdown() and len(rep.results) == 5


def test_validate_detects_count_null_and_sum_differences():
    other = [(1, 1.5, "a"), (2, 2.6, None), (3, None, None), (4, 1.0, "d")]
    rep = validate(COLS, tg("src", ROWS), [tg("x", other)])
    bad = {r.check for r in rep.results if not r.ok}
    assert bad == {"row_count", "non_null:id", "non_null:amt", "sum:amt"}  # name has 2 non-null values in both
    assert not rep.ok


def test_validate_sum_tolerance_and_query_errors():
    near = [(1, 1.5 + 1e-12, "a"), (2, 2.5, None), (3, None, "c")]
    assert validate(COLS, tg("src", ROWS), [tg("n", near)]).ok

    def boom(sql):
        raise RuntimeError("no such table")

    rep = validate(COLS, tg("src", ROWS), [Target("bad", boom, {"t": "t"})])
    assert not rep.ok and any("no such table" in e for e in rep.errors)
    rep = validate(COLS, tg("src", ROWS), [Target("unmapped", db(ROWS), {})])
    assert any("no table mapped" in e for e in rep.errors)


def test_tracer_records_nested_spans_and_errors(tmp_path):
    f = tmp_path / "t.jsonl"
    t = Tracer(f)
    set_tracer(t)
    try:
        with span("outer", password="hunter2", table="orders"):
            with span("inner") as sp:
                sp.set(rows=3)
            with pytest.raises(ValueError), span("failing"):
                raise ValueError("boom")
    finally:
        set_tracer(None)
    lines = [json.loads(x) for x in f.read_text().splitlines()]
    by = {x["name"]: x for x in lines}
    assert by["inner"]["parent"] == by["outer"]["id"] and by["inner"]["attrs"]["rows"] == 3
    assert by["failing"]["status"] == "error" and "boom" in by["failing"]["error"]
    assert by["outer"]["attrs"]["password"] == "***" and "hunter2" not in f.read_text()
    assert "inner" in t.summary()


def test_redact_hides_tokens_in_values():
    assert redact("note", "token ya29.abcDEF-123 here") == "token *** here"
    assert redact("gcs_token", "x") == "***"
    assert redact("rows", 5) == 5


def test_noop_tracer_by_default():
    set_tracer(None)
    with span("x") as sp:
        sp.set(a=1)


def test_cli_trace_flag_writes_file(tmp_path, capsys):
    f = tmp_path / "trace.jsonl"
    assert main(["--trace", str(f), "doctor"]) in (0, 1)
    names = [json.loads(x)["name"] for x in f.read_text().splitlines()]
    assert "cli.doctor" in names
    assert "Trace" in capsys.readouterr().err


def test_doctor_lists_packages_and_never_prints_env_values(monkeypatch):
    monkeypatch.setenv("SNOWFLAKE_PASSWORD", "super-secret-value")
    text = diagnostics.to_markdown(diagnostics.collect())
    assert "sqlglot:" in text and "SNOWFLAKE_PASSWORD" in text and "super-secret-value" not in text


def test_issue_url_points_at_repo_and_has_no_secret(monkeypatch):
    monkeypatch.setenv("AWS_PROFILE", "my-private-profile")
    url = diagnostics.issue_url("Crash on glue")
    assert url.startswith("https://github.com/jey1987-cmd/sql2iceberg/issues/new?template=bug_report.yml")
    assert "my-private-profile" not in url
