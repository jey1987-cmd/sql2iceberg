"""Find SQL statements in .sql files, Python files and notebooks."""

from __future__ import annotations

import ast
import json
from dataclasses import dataclass
from pathlib import Path

import sqlglot
from sqlglot.errors import ParseError

# Calls whose first argument is usually SQL text.
SQL_CALLS = {"execute", "executemany", "execute_string", "sql", "read_sql", "read_sql_query"}


@dataclass
class Statement:
    id: str
    file: str
    line: int
    sql: str
    dynamic: bool = False       # built from variables: needs parameters, not converted
    parse_error: str | None = None


def _split(sql: str) -> list[str]:
    """Split a SQL script into statements with the Snowflake parser."""
    out = []
    for expr in sqlglot.parse(sql, read="snowflake"):
        if expr is not None:
            out.append(expr.sql(dialect="snowflake", comments=False))
    return out


def detect_sql_text(text: str, file: str = "<text>", first_line: int = 1) -> list[Statement]:
    statements: list[Statement] = []
    try:
        parts = _split(text)
    except ParseError as exc:
        return [Statement(f"{file}:{first_line}", file, first_line, text.strip(), parse_error=str(exc)[:200])]
    for i, sql in enumerate(parts, 1):
        statements.append(Statement(f"{file}:{first_line}#{i}", file, first_line, sql))
    return statements


def detect_python(source: str, file: str = "<python>") -> list[Statement]:
    """Find string literals passed to cursor.execute(), spark.sql() and similar calls."""
    found: list[Statement] = []
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        return [Statement(f"{file}:1", file, 1, "", parse_error=f"python syntax error: {exc.msg}")]
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call) or not node.args:
            continue
        func = node.func
        name = func.attr if isinstance(func, ast.Attribute) else getattr(func, "id", "")
        if name not in SQL_CALLS:
            continue
        arg = node.args[0]
        if isinstance(arg, ast.Constant) and isinstance(arg.value, str):
            found.extend(detect_sql_text(arg.value, file, arg.lineno))
        elif isinstance(arg, (ast.JoinedStr, ast.BinOp)):
            found.append(Statement(f"{file}:{arg.lineno}", file, arg.lineno, "", dynamic=True))
    return found


def detect_notebook(path: Path) -> list[Statement]:
    nb = json.loads(path.read_text(encoding="utf-8"))
    found: list[Statement] = []
    for n, cell in enumerate(nb.get("cells", []), 1):
        if cell.get("cell_type") != "code":
            continue
        src = "".join(cell.get("source", []))
        src = "\n".join(l for l in src.splitlines() if not l.lstrip().startswith(("%", "!")))
        found.extend(detect_python(src, f"{path}#cell{n}"))
    return found


def detect_path(path: str | Path) -> list[Statement]:
    """Detect SQL in a file or in every supported file under a folder."""
    p = Path(path)
    files = sorted(f for f in p.rglob("*") if f.is_file()) if p.is_dir() else [p]
    found: list[Statement] = []
    for f in files:
        suffix = f.suffix.lower()
        if suffix == ".sql":
            found.extend(detect_sql_text(f.read_text(encoding="utf-8"), str(f)))
        elif suffix == ".py":
            found.extend(detect_python(f.read_text(encoding="utf-8"), str(f)))
        elif suffix == ".ipynb":
            found.extend(detect_notebook(f))
    return found
