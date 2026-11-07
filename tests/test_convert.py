import pytest

from sf2iceberg.convert import CONVERTED, REVIEW, UNSUPPORTED, convert_statement


def codes(result):
    return {f.code for f in result.findings}


def test_qualify_converts_cleanly():
    r = convert_statement(
        "SELECT id FROM orders QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1"
    )
    assert r.status == CONVERTED and "QUALIFY" not in r.spark_sql.upper()


@pytest.mark.parametrize(
    "sql,code",
    [
        ("SELECT f.value:sku::string FROM events e, LATERAL FLATTEN(input => e.payload:items) f", "FLATTEN"),
        ("SELECT k, LISTAGG(v, ',') WITHIN GROUP (ORDER BY v) FROM t GROUP BY k", "WITHIN_GROUP"),
        ("SELECT SEQ4() FROM TABLE(GENERATOR(ROWCOUNT => 10))", "GENERATOR"),
    ],
)
def test_known_invalid_spark_output_is_flagged_unsupported(sql, code):
    r = convert_statement(sql)
    assert r.status == UNSUPPORTED and code in codes(r)


@pytest.mark.parametrize(
    "sql,code",
    [
        ("SELECT DATEADD(day, 7, ts) FROM orders", "DATEADD"),
        ("SELECT a / b FROM t", "DIVIDE"),
        ("SELECT name FROM t WHERE REGEXP_LIKE(name, '^a.*')", "REGEXP_LIKE"),
        ("SELECT payload:customer.id::string FROM events", "VARIANT_PATH"),
    ],
)
def test_semantic_differences_need_review(sql, code):
    r = convert_statement(sql)
    assert r.status == REVIEW and code in codes(r)


def test_unparseable_statement_is_reported():
    r = convert_statement("SELECT ((")
    assert r.status == "unparsed"
