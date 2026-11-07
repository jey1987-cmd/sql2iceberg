"""Convert Snowflake SQL to Spark SQL with SQLGlot, then flag what cannot be trusted.

Findings come from checks against behavior observed with Spark 3.5.6 (see docs/known-issues.md).
SQLGlot does not raise for these cases, so this module adds its own checks.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

import sqlglot
from sqlglot import ErrorLevel, exp
from sqlglot.errors import ParseError

# status values
CONVERTED = "converted"          # no known problems found (still needs verification)
REVIEW = "review"                # converted, but semantics may differ from Snowflake
UNSUPPORTED = "unsupported"      # output is known not to run in Spark
UNPARSED = "unparsed"            # could not parse as Snowflake SQL


@dataclass
class Finding:
    code: str
    severity: str                # "unsupported" or "review"
    message: str


@dataclass
class Result:
    source_sql: str
    spark_sql: str | None
    status: str
    findings: list[Finding] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "source_sql": self.source_sql,
            "spark_sql": self.spark_sql,
            "status": self.status,
            "findings": [f.__dict__ for f in self.findings],
        }


# Patterns in the converted SQL that are known to fail in Spark 3.5 (confirmed by execution).
_OUTPUT_RULES = [
    (re.compile(r"EXPLODE\(\s*input\s*=>", re.IGNORECASE), "FLATTEN",
     ("FLATTEN became EXPLODE(input => ...), which Spark rejects. Rewrite with explode/posexplode "
     "on a real array column (parse JSON strings with from_json first).")),
    (re.compile(r"\bWITHIN\s+GROUP\b", re.IGNORECASE), "WITHIN_GROUP",
     ("WITHIN GROUP is not valid for collect_list or listagg in Spark 3.5. Order inside a subquery "
     "or use array_sort/sort_array with array_join.")),
    (re.compile(r"\bGET_PATH\s*\(", re.IGNORECASE), "GET_PATH",
     ("VARIANT path access stayed as GET_PATH, which Spark does not have. Use get_json_object on a JSON string "
      "or struct field access.")),
    (re.compile(r"\bMONTHNAME\s*\(", re.IGNORECASE), "MONTHNAME",
     "MONTHNAME is not a Spark function; use date_format(col, 'MMM')."),
    (re.compile(r"\b(SEQ[1248]|GENERATOR)\s*\(", re.IGNORECASE), "GENERATOR",
     "SEQ*/GENERATOR have no Spark equivalent; use range() or sequence()."),
]

_REVIEW_MESSAGES = {
    "DATEADD": "Spark DATE_ADD returns a DATE, so the time of day is dropped on timestamps. "
               "Use timestampadd or an interval for timestamp columns.",
    "DIVIDE": "Division by zero returns NULL in Spark 3.5 (ANSI off) but raises in Snowflake; "
              "Spark 4 defaults to ANSI on. Record spark.sql.ansi.enabled.",
    "TO_DATE_FORMAT": "TO_DATE format argument was dropped. Confirm the format equals the default; "
                      "bad strings give NULL in Spark but raise in Snowflake (use TRY_TO_DATE).",
    "REGEXP_LIKE": "Snowflake REGEXP_LIKE matches the whole string; Spark RLIKE matches anywhere. "
                   "Anchor the pattern (^...$) to keep the meaning.",
    "VARIANT_PATH": "VARIANT path access became get_json_object, valid only if the column holds a "
                    "JSON string, not a struct or map.",
    "OBJECT_CONSTRUCT": "OBJECT_CONSTRUCT became STRUCT; null handling and dynamic keys differ.",
}


def _source_checks(expr: exp.Expression) -> list[Finding]:
    found: list[Finding] = []
    codes: set[str] = set()
    for node in expr.walk():
        if isinstance(node, exp.DateAdd):
            codes.add("DATEADD")
        elif isinstance(node, exp.Div):
            codes.add("DIVIDE")
        elif isinstance(node, exp.RegexpLike):
            codes.add("REGEXP_LIKE")
        elif isinstance(node, (exp.TsOrDsToDate, exp.StrToDate)) and node.args.get("format"):
            codes.add("TO_DATE_FORMAT")
        elif isinstance(node, (exp.JSONExtract, exp.JSONBExtract)):
            codes.add("VARIANT_PATH")
        elif isinstance(node, (exp.Struct,)) or (
            isinstance(node, exp.Anonymous) and node.name.upper() == "OBJECT_CONSTRUCT"
        ):
            codes.add("OBJECT_CONSTRUCT")
    for code in sorted(codes):
        found.append(Finding(code, "review", _REVIEW_MESSAGES[code]))
    return found


class _Capture(logging.Handler):
    """Collect SQLGlot's warnings about arguments it cannot express in the target dialect."""

    def __init__(self) -> None:
        super().__init__(level=logging.WARNING)
        self.messages: list[str] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.messages.append(record.getMessage())


def convert_statement(sql: str) -> Result:
    """Convert one Snowflake statement and report findings."""
    try:
        expr = sqlglot.parse_one(sql, read="snowflake")
    except ParseError as exc:
        return Result(sql, None, UNPARSED, [Finding("PARSE", "unsupported", str(exc)[:200])])
    cap = _Capture()
    logger = logging.getLogger("sqlglot")
    logger.addHandler(cap)
    try:
        spark = expr.sql(dialect="spark", unsupported_level=ErrorLevel.WARN, comments=False)
    finally:
        logger.removeHandler(cap)
    findings: list[Finding] = []
    for msg in dict.fromkeys(cap.messages):
        findings.append(Finding("DROPPED", "review",
                                f"SQLGlot could not express part of this in Spark and dropped it: {msg[:160]}. "
                                "The result can differ from Snowflake."))
    for pattern, code, message in _OUTPUT_RULES:
        if pattern.search(spark):
            findings.append(Finding(code, "unsupported", message))
    findings.extend(_source_checks(expr))
    try:
        sqlglot.parse_one(spark, read="spark")
    except ParseError as exc:
        findings.append(Finding("SPARK_PARSE", "unsupported", f"output does not parse as Spark SQL: {str(exc)[:160]}"))
    if any(f.severity == "unsupported" for f in findings):
        status = UNSUPPORTED
    elif findings:
        status = REVIEW
    else:
        status = CONVERTED
    return Result(sql, spark, status, findings)
