from pathlib import Path

from sf2iceberg.detect import detect_path, detect_python, detect_sql_text

EX = Path(__file__).parent.parent / "examples"


def test_sql_file_is_split_into_statements():
    found = detect_path(EX / "sample.sql")
    assert len(found) == 4
    assert all(not s.dynamic and not s.parse_error for s in found)


def test_python_calls_and_dynamic_sql():
    found = detect_python((EX / "app.py").read_text(), "app.py")
    assert sum(1 for s in found if s.dynamic) == 1
    assert any("IFF(" in s.sql for s in found if not s.dynamic)


def test_non_sql_calls_are_ignored():
    assert detect_python("print('select 1')\nx = 'SELECT 2'") == []


def test_unparseable_sql_is_reported_not_raised():
    found = detect_sql_text("SELEC FROM WHERE ((", "bad.sql")
    assert found and (found[0].parse_error or found[0].sql)
