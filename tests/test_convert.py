import pytest

from sql2iceberg.convert import CONVERTED, REVIEW, UNSUPPORTED, convert_statement


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
        ("SELECT f.key FROM events e, LATERAL FLATTEN(input => e.payload:items) f", "FLATTEN"),
        ("SELECT k, LISTAGG(v, ',') WITHIN GROUP (ORDER BY v ASC, w DESC) FROM t GROUP BY k", "WITHIN_GROUP"),
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


@pytest.mark.parametrize(
    "sql,expected",
    [
        ("SELECT TRY_TO_NUMBER(c) FROM t", "TRY_CAST(c AS DECIMAL(38, 0))"),
        ("SELECT TRY_TO_DECIMAL(c, 10, 2) FROM t", "TRY_CAST(c AS DECIMAL(10, 2))"),
        ("SELECT TRY_TO_DOUBLE(c) FROM t", "TRY_CAST(c AS DOUBLE)"),
        ("SELECT TRY_TO_DATE(c) FROM t", "TRY_TO_DATE(c)"),
        ("SELECT TRY_TO_DATE(c, 'yyyymmdd') FROM t", "TRY_TO_DATE(c, 'yyyyMMdd')"),
    ],
)
def test_try_functions_stay_safe(sql, expected):
    r = convert_statement(sql)
    assert expected in r.spark_sql and "DROPPED" not in codes(r)


def test_plain_to_date_is_not_made_safe():
    assert "TRY_" not in convert_statement("SELECT TO_DATE(c) FROM t").spark_sql


def test_flatten_value_becomes_lateral_view_explode():
    r = convert_statement("SELECT f.value:sku::string AS sku FROM events e, LATERAL FLATTEN(input => e.payload:items) f")
    assert r.status == REVIEW and "FLATTEN_JSON" in codes(r)
    assert "LATERAL VIEW EXPLODE(FROM_JSON(GET_JSON_OBJECT(e.payload, '$.items'), 'array<string>')) f AS value" in r.spark_sql


def test_listagg_within_group_becomes_sorted_array_join():
    r = convert_statement("SELECT k, LISTAGG(DISTINCT v, ',') WITHIN GROUP (ORDER BY v) FROM t GROUP BY k")
    assert r.status == CONVERTED and "WITHIN GROUP" not in r.spark_sql
    assert "SORT_ARRAY(COLLECT_SET(v), TRUE)" in r.spark_sql and "ARRAY_JOIN" in r.spark_sql


def test_array_agg_within_group_inside_array_to_string():
    r = convert_statement("SELECT ARRAY_TO_STRING(ARRAY_AGG(id) WITHIN GROUP (ORDER BY id), '|') FROM t")
    assert r.status == CONVERTED and "WITHIN GROUP" not in r.spark_sql and "SORT_ARRAY(COLLECT_LIST(id), TRUE)" in r.spark_sql


def test_percentile_cont_is_exact_not_approximate():
    r = convert_statement("SELECT PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount) FROM o")
    assert "PERCENTILE(amount, 0.9)" in r.spark_sql and "APPROX" not in r.spark_sql


def test_datediff_counts_boundaries_like_snowflake():
    r = convert_statement("SELECT DATEDIFF('day', a, b), DATEDIFF(month, a, b) FROM o")
    assert "DATEDIFF(DAY, CAST(a AS DATE), CAST(b AS DATE))" in r.spark_sql
    assert "DATEDIFF(MONTH, DATE_TRUNC('MONTH', a), DATE_TRUNC('MONTH', b))" in r.spark_sql


def test_array_size_of_variant_path_parses_json_first():
    r = convert_statement("SELECT ARRAY_SIZE(p:items) FROM o")
    assert "SIZE(FROM_JSON(GET_JSON_OBJECT(p, '$.items'), 'array<string>'))" in r.spark_sql
