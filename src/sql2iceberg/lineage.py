"""Map the inputs and outputs of each SQL statement (which tables it reads and writes)."""

from __future__ import annotations

import sqlglot
from sqlglot import exp
from sqlglot.errors import ParseError

_WRITERS = (exp.Create, exp.Insert, exp.Merge, exp.Delete, exp.Update)


def _name(t: exp.Table) -> str:
    return ".".join(p.name for p in (t.args.get("catalog"), t.args.get("db"), t.this) if p)


def map_statement(sql: str, dialect: str = "snowflake") -> dict:
    """Return {kind, outputs, inputs} for one statement. Table names are as written."""
    try:
        tree = sqlglot.parse_one(sql, read=dialect)
    except ParseError as exc:
        return {"kind": "unparsed", "outputs": [], "inputs": [], "error": str(exc)[:160]}
    kind = type(tree).__name__.upper()
    outputs: list[str] = []
    if isinstance(tree, _WRITERS):
        target = tree.this.this if isinstance(tree, exp.Create) and isinstance(tree.this, exp.Schema) else tree.this
        if isinstance(target, exp.Table):
            outputs.append(_name(target))
    ctes = {c.alias for c in tree.find_all(exp.CTE)}
    target_ids = {id(tree.this)} | {id(tree.this.this)} if isinstance(tree, _WRITERS) else set()
    inputs: list[str] = []
    for t in tree.find_all(exp.Table):
        n = _name(t)
        if not n or n in ctes or id(t) in target_ids:
            continue
        if isinstance(t.this, exp.Anonymous) or not isinstance(t.this, exp.Identifier):
            continue  # table functions such as GENERATOR(...)
        if n not in inputs:
            inputs.append(n)
    return {"kind": kind, "outputs": outputs, "inputs": inputs}


def map_statements(statements, dialect: str = "snowflake") -> dict:
    """Build a lineage map over detected statements: per statement plus per table."""
    items, tables = [], {}
    for s in statements:
        if s.dynamic or s.parse_error:
            items.append({"id": s.id, "kind": "dynamic" if s.dynamic else "unparsed", "inputs": [], "outputs": []})
            continue
        m = map_statement(s.sql, dialect)
        items.append({"id": s.id, **m})
        for n in m["inputs"]:
            tables.setdefault(n, {"read_by": [], "written_by": []})["read_by"].append(s.id)
        for n in m["outputs"]:
            tables.setdefault(n, {"read_by": [], "written_by": []})["written_by"].append(s.id)
    # external = read but never written inside the scanned code: these need to exist before the run
    sources = sorted(n for n, v in tables.items() if v["read_by"] and not v["written_by"])
    targets = sorted(n for n, v in tables.items() if v["written_by"])
    return {"statements": items, "tables": tables, "source_tables": sources, "target_tables": targets}
