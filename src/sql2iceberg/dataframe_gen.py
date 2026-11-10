"""Plan the PySpark code for one statement: Spark SQL when it is simple, DataFrame steps mixed with Spark SQL when it is not.

Input is the Spark SQL produced by ``convert_statement`` after table names point at the Iceberg catalog.
Only ``CREATE TABLE ... AS SELECT`` and ``INSERT ... SELECT`` can be restructured; every other statement is plain Spark SQL.

How a statement is chosen (``mode="auto"``):

* **simple**  (one SELECT, at most one join, no CTE, subquery, window, set operation, PIVOT or LATERAL VIEW) -> ``spark.sql``
* **complex** -> every CTE becomes its own DataFrame step. A step is built with the DataFrame API (``join``, ``where``,
  ``groupBy().agg()``, ``select``, ``union``, ``orderBy``, ``limit``, pivot, explode) when its shape is supported; expressions
  stay as ``F.expr("...")`` so function behaviour is exactly the converted Spark SQL. A step with a shape the builder cannot
  express (recursive CTE, ROLLUP, window over an aggregate, ...) is run with ``spark.sql`` against the earlier steps, which are
  registered as temp views. The result is written with ``writeTo`` (CTAS) or the ``append_df`` helper (INSERT).

``mode="sql"`` forces Spark SQL everywhere, ``mode="mixed"`` forces the DataFrame builder for every CTAS/INSERT.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

from sqlglot import exp

SETOPS = tuple(c for c in (getattr(exp, "SetOperation", None), getattr(exp, "Union", None)) if c)

HELPERS = {
    "pivot_df": '''def pivot_df(df, column, values, agg, agg_columns, names=None):
    """SQL PIVOT: group by every column except the pivot column and the aggregated ones, one output column per value."""
    keys = [c for c in df.columns if c not in {column, *agg_columns}]
    out = df.groupBy(*keys).pivot(column, values).agg(agg)
    return out.toDF(*names) if names else out
''',
    "append_df": '''def append_df(df, table, columns=None):
    """INSERT ... SELECT: columns are matched by position; columns not listed are filled with NULL."""
    target = spark.table(table).columns
    columns = columns or target[: len(df.columns)]
    df = df.toDF(*columns)
    for missing in (c for c in target if c.lower() not in {x.lower() for x in columns}):
        df = df.withColumn(missing, F.lit(None))
    df.select(*target).writeTo(table).append()
''',
}


@dataclass
class Plan:
    lines: list[str]
    strategy: str                       # "spark_sql" | "mixed"
    reasons: list[str] = field(default_factory=list)
    df_steps: int = 0
    sql_steps: int = 0
    helpers: set[str] = field(default_factory=set)
    uses_functions: bool = False


def complexity(query: exp.Expression, statement: exp.Expression | None = None) -> list[str]:
    """Why a query is not simple (empty list = simple)."""
    why: list[str] = []
    ctes = [w for node in (query, statement) if node is not None for w in [node.args.get("with_")] if w]
    if ctes:
        why.append("CTE")
    if any(isinstance(n, (exp.Subquery, exp.Exists)) or (isinstance(n, exp.In) and n.args.get("query"))
           for n in query.walk() if n is not query):
        why.append("subquery")
    if isinstance(query, SETOPS) or any(isinstance(n, SETOPS) for n in query.walk()):
        why.append("set operation")
    if any(isinstance(n, exp.Window) for n in query.walk()):
        why.append("window function")
    if any(isinstance(n, exp.Pivot) for n in query.walk()):
        why.append("pivot")
    if any(isinstance(n, exp.Lateral) for n in query.walk()):
        why.append("lateral view")
    if len(list(query.find_all(exp.Join))) >= 2:
        why.append("several joins")
    return why


def _text(e: exp.Expression) -> str:
    return e.sql(dialect="spark", comments=False)


def _norm(e: exp.Expression) -> str:
    c = e.copy()
    for col in c.find_all(exp.Column):
        col.set("table", None)
    return _text(c)


def _has_agg(e: exp.Expression) -> bool:
    return any(isinstance(n, exp.AggFunc) and n.find_ancestor(exp.Window) is None for n in e.walk())


def _has_window(e: exp.Expression) -> bool:
    return any(isinstance(n, exp.Window) for n in e.walk())


def _ident(name: str) -> str:
    return re.sub(r"\W+", "_", name).strip("_").lower() or "step"


def _chain(parts: list[str], indent: str = "    ") -> str:
    if len(parts) == 1:
        return parts[0]
    return "(\n" + "\n".join(indent + p for p in parts) + "\n)"


class _Builder:
    """Translate SELECTs into DataFrame method chains. Every translate method returns None when the shape is unsupported."""

    def __init__(self) -> None:
        self.ctes: dict[str, str] = {}          # cte name -> python variable
        self.lateral_aliases: set[str] = set()
        self.subquery_in_expr = False
        self.helpers: set[str] = set()

    # ---- expressions ----
    def expr(self, e: exp.Expression) -> str:
        c = e.copy()
        if any(isinstance(n, (exp.Subquery, exp.Select)) for n in c.walk()):
            self.subquery_in_expr = True
        for col in c.find_all(exp.Column):
            if col.table and col.table.lower() in self.lateral_aliases:
                col.set("table", None)
        return f"F.expr({_text(c)!r})"

    # ---- sources ----
    def source(self, node: exp.Expression) -> list[str] | None:
        if isinstance(node, exp.Table):
            if not isinstance(node.this, exp.Identifier):
                return None
            name = node.name
            if not node.args.get("db") and name.lower() in self.ctes:
                base = self.ctes[name.lower()]
            else:
                full = ".".join(p.name for p in (node.args.get("catalog"), node.args.get("db"), node.this) if p)
                base = f"spark.table({full!r})"
            parts = [f"{base}.alias({(node.alias or name)!r})"]
            pivots = node.args.get("pivots")
        elif isinstance(node, exp.Subquery):
            inner = self.query(node.this)
            if inner is None:
                return None
            parts = [f"{''.join(inner)}"]
            alias = node.args.get("alias")
            if alias is not None and alias.args.get("columns"):
                parts[0] += f".toDF({', '.join(repr(c.name) for c in alias.args['columns'])})"
            if alias is not None and alias.name:
                parts[0] += f".alias({alias.name!r})"
            pivots = node.args.get("pivots")
        else:
            return None
        if pivots:
            return self.pivot(parts, pivots)
        return parts

    def pivot(self, parts: list[str], pivots: list[exp.Pivot]) -> list[str] | None:
        if len(pivots) != 1:
            return None
        p = pivots[0]
        if p.args.get("unpivot") or len(p.expressions) != 1 or len(p.args.get("fields") or []) != 1:
            return None
        field_in = p.args["fields"][0]
        if not isinstance(field_in, exp.In) or not isinstance(field_in.this, exp.Column):
            return None
        values = []
        for v in field_in.expressions:
            lit = v.this if isinstance(v, exp.PivotAlias) else v
            if not isinstance(lit, exp.Literal):
                return None
            values.append(lit.name if lit.is_string else (float(lit.name) if "." in lit.name else int(lit.name)))
        agg = p.expressions[0]
        agg_cols = sorted({c.name for c in agg.find_all(exp.Column)})
        alias = p.args.get("alias")
        names = [c.name for c in alias.args["columns"]] if alias is not None and alias.args.get("columns") else None
        self.helpers.add("pivot_df")
        call = (f"pivot_df({''.join(parts)}, {field_in.this.name!r}, {values!r}, {self.expr(agg)}, "
                f"{agg_cols!r}{', ' + repr(names) if names else ''})")
        return [call + (f".alias({alias.name!r})" if alias is not None and alias.name else "")]

    # ---- queries ----
    def query(self, q: exp.Expression) -> list[str] | None:
        if isinstance(q, exp.Subquery):
            return self.query(q.this)
        if isinstance(q, SETOPS):
            return self.setop(q)
        if isinstance(q, exp.Select):
            return self.select(q)
        return None

    def setop(self, q: exp.Expression) -> list[str] | None:
        if q.args.get("order") or q.args.get("limit") or q.args.get("with_"):
            return None
        left, right = self.query(q.this), self.query(q.expression)
        if left is None or right is None:
            return None
        distinct = q.args.get("distinct", True)
        kind = type(q).__name__.lower()
        method = {"union": "union", "intersect": "intersect" if distinct else "intersectAll",
                  "except": "subtract" if distinct else "exceptAll"}.get(kind)
        if method is None:
            return None
        parts = [''.join(left), f".{method}({''.join(right)})"]
        if kind == "union" and distinct:
            parts.append(".distinct()")
        return parts

    def select(self, sel: exp.Select) -> list[str] | None:
        if sel.args.get("with_"):
            return None
        for key in ("qualify", "windows", "sample", "connect", "match", "distribute", "cluster", "sort", "into", "offset",
                    "prewhere", "for_", "kind", "hint"):
            if sel.args.get(key):
                return None
        src = sel.args.get("from_")
        if src is None:
            return None
        parts = self.source(src.this)
        if parts is None:
            return None
        for j in sel.args.get("joins") or []:
            right = self.source(j.this)
            if right is None or j.args.get("method"):
                return None
            side, kind = (j.args.get("side") or "").upper(), (j.args.get("kind") or "").upper()
            on, using = j.args.get("on"), j.args.get("using")
            if kind == "CROSS" or (on is None and not using):
                parts.append(f".crossJoin({''.join(right)})")
                continue
            how = {"LEFT": "left", "RIGHT": "right", "FULL": "full"}.get(side, "inner")
            if kind == "SEMI":
                how = "left_semi"
            elif kind == "ANTI":
                how = "left_anti"
            cond = repr([i.name for i in using]) if using else self.expr(on)
            parts.append(f".join({''.join(right)}, {cond}, {how!r})")
        for lat in sel.args.get("laterals") or []:
            if not (isinstance(lat, exp.Lateral) and lat.args.get("view")):
                return None
            call = lat.this
            fn = call.name.upper() if isinstance(call, exp.Anonymous) else type(call).__name__.upper()
            args = call.expressions if isinstance(call, exp.Anonymous) else [call.this]
            alias = lat.args.get("alias")
            if fn not in ("EXPLODE", "POSEXPLODE") or alias is None or len(args) != 1:
                return None
            cols = [c.name for c in alias.args.get("columns") or []]
            outer = "_outer" if lat.args.get("outer") else ""
            func = ("posexplode" if fn == "POSEXPLODE" else "explode") + outer
            parts.append(f".select(F.expr('*'), F.{func}({self.expr(args[0])}).alias({', '.join(map(repr, cols))}))")
            self.lateral_aliases.add(alias.name.lower())
        where = sel.args.get("where")
        if where is not None:
            parts.append(f".where({self.expr(where.this)})")
        items = list(sel.expressions)
        group = sel.args.get("group")
        having = sel.args.get("having")
        aggregated = group is not None or any(_has_agg(i) for i in items)
        if aggregated:
            out = self.aggregate(sel, items, group, having, parts)
            if out is None:
                return None
            names = out
        else:
            if having is not None:
                return None
            names = [i.alias_or_name for i in items]
            parts.append(f".select({', '.join(self.expr(i) for i in items)})")
        if sel.args.get("distinct"):
            if sel.args["distinct"].args.get("on"):
                return None
            parts.append(".distinct()")
        order = sel.args.get("order")
        if order is not None:
            sort = self.order(order, names)
            if sort is None:
                return None
            parts.append(sort)
        limit = sel.args.get("limit")
        if limit is not None:
            if not isinstance(limit.expression, exp.Literal):
                return None
            parts.append(f".limit({limit.expression.name})")
        return parts

    def aggregate(self, sel, items, group, having, parts) -> list[str] | None:
        if any(isinstance(i, exp.Star) or (isinstance(i, exp.Column) and isinstance(i.this, exp.Star)) for i in items):
            return None
        if group is not None and any(group.args.get(k) for k in ("rollup", "cube", "grouping_sets", "totals")):
            return None
        keys: list[exp.Expression] = []
        for g in (group.expressions if group is not None else []):
            if isinstance(g, exp.Literal) and not g.is_string:
                idx = int(g.name) - 1
                if not 0 <= idx < len(items):
                    return None
                g = items[idx].this if isinstance(items[idx], exp.Alias) else items[idx]
            keys.append(g)
        names = [i.alias_or_name for i in items]
        if len(set(names)) != len(names) or "" in names:
            return None
        key_cols, agg_cols = [], []
        selected_keys: dict[str, str] = {}      # normalised key -> output name
        for item, name in zip(items, names):
            inner = item.this if isinstance(item, exp.Alias) else item
            if _has_window(inner):
                return None
            if _has_agg(inner):
                agg_cols.append(f"{self.expr(inner)}.alias({name!r})")
            elif any(_norm(inner) == _norm(k) for k in keys):
                selected_keys.setdefault(_norm(inner), name)
                key_cols.append(f"{self.expr(inner)}.alias({name!r})")
            else:
                return None
        for n, k in enumerate(keys):                    # grouped but not selected
            if _norm(k) not in selected_keys:
                selected_keys[_norm(k)] = f"_g{n}"
                key_cols.append(f"{self.expr(k)}.alias('_g{n}')")
        if having is not None:
            agg_cols.append(f"{self.expr(having.this)}.alias('_having')")
        if key_cols:
            parts.append(f".groupBy({', '.join(key_cols)})")
            parts.append(f".agg({', '.join(agg_cols)})" if agg_cols else ".agg(F.lit(1).alias('_one'))")
        else:
            parts.append(f".agg({', '.join(agg_cols)})")
        if having is not None:
            parts.append(".where(F.col('_having'))")
        parts.append(f".select({', '.join(f'F.col({chr(96) + n + chr(96)!r})' for n in names)})")
        return names

    def order(self, order: exp.Order, names: list[str]) -> str | None:
        cols = []
        lowered = {n.lower() for n in names}
        for o in order.expressions:
            if isinstance(o.this, exp.Literal) or any(c.name.lower() not in lowered for c in o.this.find_all(exp.Column)):
                return None
            desc, nulls_first = bool(o.args.get("desc")), o.args.get("nulls_first")
            method = ("desc" if desc else "asc") + ("" if nulls_first is None else "_nulls_first" if nulls_first else "_nulls_last")
            cols.append(f"{self.expr(o.this)}.{method}()")
        return f".orderBy({', '.join(cols)})"


def _target(node: exp.Expression) -> tuple[str, list[str]] | None:
    """(table name, listed columns) of the table a CREATE/INSERT writes."""
    cols: list[str] = []
    if isinstance(node, exp.Schema):
        cols = [c.name for c in node.expressions]
        node = node.this
    if not isinstance(node, exp.Table):
        return None
    return ".".join(p.name for p in (node.args.get("catalog"), node.args.get("db"), node.this) if p), cols


def plan_statement(tree: exp.Expression, sql_text: str, mode: str = "auto") -> Plan:
    """Plan one statement. ``tree`` is the Iceberg-qualified Spark syntax tree, ``sql_text`` its (rewritten) SQL text.

    Never raises for an unusual query shape: if the DataFrame builder fails for any reason the statement is kept as Spark SQL,
    which always carries the full converted logic.
    """
    try:
        return _plan(tree, sql_text, mode)
    except Exception as exc:  # noqa: BLE001  deliberate catch-all: the Spark SQL form is always a valid answer
        return Plan([f'spark.sql("""{sql_text}""")'], "spark_sql", [f"kept as Spark SQL ({type(exc).__name__})"], sql_steps=1)


def _plan(tree: exp.Expression, sql_text: str, mode: str) -> Plan:
    sql_plan = Plan([f'spark.sql("""{sql_text}""")'], "spark_sql", sql_steps=1)
    is_ctas = isinstance(tree, exp.Create) and tree.expression is not None and (tree.args.get("kind") or "").upper() == "TABLE"
    is_insert = isinstance(tree, exp.Insert) and tree.expression is not None and not isinstance(tree.expression, exp.Values)
    if mode == "sql" or not (is_ctas or is_insert):
        return sql_plan
    query = tree.expression
    why = complexity(query, tree)
    with_ = tree.args.get("with_") or query.args.get("with_")
    if with_ is not None and with_.args.get("recursive"):
        sql_plan.reasons = [*why, "recursive CTE: kept as Spark SQL"]
        return sql_plan
    if mode == "auto" and not why:
        return sql_plan
    if is_insert and tree.args.get("overwrite"):
        sql_plan.reasons = [*why, "INSERT OVERWRITE: kept as Spark SQL"]
        return sql_plan
    target = _target(tree.this)
    if target is None:
        return sql_plan

    b = _Builder()
    steps: list[tuple[str, str, bool]] = []      # (python variable, code, is_sql)
    ctes = list(with_.expressions) if with_ is not None else []
    body = query.copy()
    body.set("with_", None)
    for cte in ctes:
        name = cte.alias
        var = f"df_{_ident(name)}"
        chain = b.query(cte.this)
        is_sql = chain is None
        code = f'spark.sql("""{_text(cte.this)}""")' if is_sql else _chain(chain)
        cols = [c.name for c in (cte.args.get("alias").args.get("columns") or [])] if cte.args.get("alias") else []
        if cols:
            code = f"{code}.toDF({', '.join(map(repr, cols))})"
        b.ctes[name.lower()] = var
        steps.append((var, code, is_sql))
    chain = b.query(body)
    final_sql = chain is None
    final_code = f'spark.sql("""{_text(body)}""")' if final_sql else _chain(chain)
    steps.append(("result", final_code, final_sql))

    need_views = any(s[2] for s in steps) or b.subquery_in_expr
    lines: list[str] = []
    for (var, code, is_sql), cte in zip(steps, [*ctes, None]):
        label = f"CTE {cte.alias}" if cte is not None else "final SELECT"
        lines.append(f"# {label}: {'Spark SQL' if is_sql else 'DataFrame API'}")
        lines.append(f"{var} = {code}")
        if cte is not None and need_views:
            lines.append(f"{var}.createOrReplaceTempView({cte.alias.lower()!r})")
    table, listed = target
    if is_ctas:
        writer = f"result.writeTo({table!r}).using('iceberg').tableProperty('format-version', str(FORMAT_VERSION))"
        if tree.args.get("exists"):
            lines += [f"if not spark.catalog.tableExists({table!r}):", f"    {writer}.create()"]
        else:
            lines.append(f"{writer}.{'createOrReplace' if tree.args.get('replace') else 'create'}()")
    else:
        b.helpers.add("append_df")
        lines.append(f"append_df(result, {table!r}{', ' + repr(listed) if listed else ''})")
    df_steps = sum(1 for s in steps if not s[2])
    return Plan(lines, "mixed", why or ["forced by mode=mixed"], df_steps, len(steps) - df_steps, b.helpers, True)
