"""Build the conversion report (JSON and readable Markdown)."""

from __future__ import annotations

import json
from collections import Counter

from .convert import Result
from .detect import Statement


def build(statements: list[Statement], results: dict[str, Result]) -> dict:
    items = []
    for s in statements:
        item = {"id": s.id, "file": s.file, "line": s.line}
        if s.dynamic:
            item.update(status="dynamic", note="built from variables; needs parameters")
        elif s.parse_error:
            item.update(status="unparsed", note=s.parse_error)
        else:
            item.update(results[s.id].to_dict())
        items.append(item)
    counts = Counter(i["status"] for i in items)
    return {"summary": dict(counts), "total": len(items), "statements": items}


def to_json(report: dict) -> str:
    return json.dumps(report, indent=2)


def to_markdown(report: dict) -> str:
    lines = ["# sf2iceberg conversion report", "", f"Statements found: {report['total']}", ""]
    for status, n in sorted(report["summary"].items()):
        lines.append(f"- {status}: {n}")
    lines.append("")
    for item in report["statements"]:
        lines.append(f"## {item['id']} ({item['status']})")
        if item.get("source_sql"):
            lines += ["", "```sql", item["source_sql"], "```"]
        if item.get("spark_sql"):
            lines += ["", "Spark SQL:", "```sql", item["spark_sql"], "```"]
        for f in item.get("findings", []):
            lines.append(f"- **{f['code']}** ({f['severity']}): {f['message']}")
        if item.get("note"):
            lines.append(f"- {item['note']}")
        lines.append("")
    lines.append("Statuses mean: converted = no known problems found (still verify); "
                 "review = semantics may differ from Snowflake; unsupported = known not to run in Spark.")
    return "\n".join(lines)
