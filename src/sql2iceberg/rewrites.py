"""Rewrite Snowflake constructs that SQLGlot leaves in a form Spark rejects, or runs with a different result.

Applied to the Snowflake syntax tree before it is written as Spark SQL (see ``convert.convert_statement``):

* ``LISTAGG`` / ``ARRAY_AGG`` ... ``WITHIN GROUP (ORDER BY ...)``  -> sorted ``collect_list``/``collect_set`` (+ ``array_join``)
* ``PERCENTILE_CONT(p) WITHIN GROUP (ORDER BY x)``                 -> exact ``percentile(x, p)`` (SQLGlot gives the approximate one)
* ``DATEDIFF(day|month|quarter|year, a, b)``                       -> boundary counting, as Snowflake does
* ``ARRAY_SIZE(variant:path)``                                     -> ``size(from_json(...))``
* ``ARRAY_TO_STRING(array, sep)``                                  -> element-wise cast to string first
* ``LATERAL FLATTEN(input => col) f``                              -> ``LATERAL VIEW [POS]EXPLODE(from_json(col)) f``
"""

from __future__ import annotations

from sqlglot import exp


def json_array(node: exp.Expression) -> exp.Expression:
    """VARIANT/ARRAY columns reach Spark as JSON text (JDBC): parse them into a real array first."""
    return exp.Anonymous(this="FROM_JSON", expressions=[node.copy(), exp.Literal.string("array<string>")])


def as_string_lambda(array: exp.Expression) -> exp.Expression:
    """TRANSFORM(array, x -> CAST(x AS STRING)): ARRAY_JOIN needs string elements (strict under Spark 4 ANSI)."""
    x = exp.to_identifier("x")
    body = exp.Cast(this=exp.column("x"), to=exp.DataType.build("STRING"))
    return exp.Anonymous(this="TRANSFORM", expressions=[array, exp.Lambda(this=body, expressions=[x])])


def _order_keys(order: exp.Expression | None) -> list[tuple[exp.Expression, bool]] | None:
    """[(expression, descending)] of a WITHIN GROUP / ORDER BY, or None when directions are mixed."""
    if order is None:
        return []
    keys = [(o.this.copy(), bool(o.args.get("desc"))) for o in order.expressions]
    return keys if len({d for _, d in keys}) <= 1 else None


def _sorted_array(values: exp.Expression, distinct: bool, keys: list[tuple[exp.Expression, bool]]) -> exp.Expression:
    """collect_list/collect_set of ``values`` ordered by ``keys`` (what WITHIN GROUP does in Snowflake)."""
    collect = "COLLECT_SET" if distinct else "COLLECT_LIST"
    collected = exp.Anonymous(this=collect, expressions=[values.copy()])
    if not keys:
        return collected
    asc = exp.Boolean(this=not keys[0][1])
    if len(keys) == 1 and keys[0][0].sql() == values.sql():
        return exp.Anonymous(this="SORT_ARRAY", expressions=[collected, asc])
    # order by a different expression: sort structs by the key fields, then take the value field
    fields = [exp.alias_(k.copy(), f"k{i}") for i, (k, _) in enumerate(keys)] + [exp.alias_(values.copy(), "v")]
    packed = exp.Anonymous(this="SORT_ARRAY", expressions=[
        exp.Anonymous(this=collect, expressions=[exp.Anonymous(this="STRUCT", expressions=fields)]), asc])
    s = exp.to_identifier("s")
    return exp.Anonymous(this="TRANSFORM", expressions=[
        packed, exp.Lambda(this=exp.Column(this=exp.to_identifier("v"), table=s), expressions=[s])])


def spark_semantics(node: exp.Expression) -> exp.Expression:
    """Expression-level rewrites; use with ``tree.transform``."""
    if isinstance(node, exp.WithinGroup) and isinstance(node.this, exp.PercentileCont):
        keys = _order_keys(node.args.get("expression"))
        p = node.this.this
        if keys and len(keys) == 1:
            x, desc = keys[0]
            if desc and isinstance(p, exp.Literal) and not p.is_string:
                p = exp.Literal.number(round(1 - float(p.name), 12))
            return exp.Anonymous(this="PERCENTILE", expressions=[x, p.copy()])
    if isinstance(node, exp.WithinGroup) and isinstance(node.this, exp.ArrayAgg):
        keys = _order_keys(node.args.get("expression"))
        if keys is not None:
            return _sorted_array(node.this.this, False, keys)
    if isinstance(node, exp.GroupConcat):
        inner, keys = node.this, []
        if isinstance(inner, exp.Order):
            ordered = _order_keys(inner)
            if ordered is None:
                return node
            keys, inner = ordered, inner.this
        distinct = False
        if isinstance(inner, exp.Distinct) and len(inner.expressions) == 1:
            distinct, inner = True, inner.expressions[0]
        if isinstance(inner, exp.Distinct) or inner is None:
            return node
        sep = node.args.get("separator") or exp.Literal.string("")
        return exp.Anonymous(this="ARRAY_JOIN", expressions=[as_string_lambda(_sorted_array(inner, distinct, keys)), sep.copy()])
    if isinstance(node, exp.ArrayToString):
        first = node.this
        if not (isinstance(first, exp.Anonymous) and first.name.upper() == "TRANSFORM"):
            return exp.ArrayToString(this=as_string_lambda(first.copy()), expression=node.expression.copy())
    if isinstance(node, exp.ArraySize) and isinstance(node.this, (exp.JSONExtract, exp.JSONBExtract)):
        return exp.ArraySize(this=json_array(node.this))
    if isinstance(node, exp.DateDiff) and node.args.get("unit") is not None and not node.args.get("sql2iceberg_fixed"):
        unit = node.args["unit"].name.upper()
        a, b = node.expression, node.this
        if a is not None and b is not None and unit in ("DAY", "MONTH", "QUARTER", "YEAR"):
            def norm(e: exp.Expression) -> exp.Expression:
                if unit == "DAY":
                    return exp.Cast(this=e.copy(), to=exp.DataType.build("DATE"))
                return exp.Anonymous(this="DATE_TRUNC", expressions=[exp.Literal.string(unit), e.copy()])
            fixed = exp.DateDiff(this=norm(b), expression=norm(a), unit=node.args["unit"].copy())
            fixed.set("sql2iceberg_fixed", True)
            return fixed
    return node


def flatten_to_lateral_view(tree: exp.Expression) -> bool:
    """``FROM t, LATERAL FLATTEN(input => t.col) f`` -> ``FROM t LATERAL VIEW POSEXPLODE(from_json(t.col)) f AS index, value``.

    Only the VALUE and INDEX columns are supported; a FLATTEN that uses KEY, PATH, SEQ or THIS is left alone so the
    output check reports it. Returns True if anything was rewritten.
    """
    changed = False
    for select in list(tree.find_all(exp.Select)):
        keep, views = [], []
        for join in select.args.get("joins") or []:
            lat = join.this
            ok = (isinstance(lat, exp.Lateral) and isinstance(lat.this, exp.Explode)
                  and isinstance(lat.this.this, exp.Kwarg) and lat.this.this.this.name.lower() == "input"
                  and lat.args.get("alias") is not None)
            if not ok:
                keep.append(join)
                continue
            alias = lat.args["alias"].name
            used = {c.name.lower() for c in select.find_all(exp.Column) if c.table.lower() == alias.lower()}
            if used - {"value", "index"}:
                keep.append(join)
                continue
            func = "POSEXPLODE" if "index" in used else "EXPLODE"
            cols = ["index", "value"] if func == "POSEXPLODE" else ["value"]
            views.append(exp.Lateral(
                this=exp.Anonymous(this=func, expressions=[json_array(lat.this.this.expression)]), view=True, outer=False,
                alias=exp.TableAlias(this=exp.to_identifier(alias), columns=[exp.to_identifier(c) for c in cols])))
        if views:
            select.set("joins", keep or None)
            select.set("laterals", (select.args.get("laterals") or []) + views)
            changed = True
    return changed
