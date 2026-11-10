"""Render the visuals for the Towards Data Science guide (Pillow only, same palette as the other article images).

    python docs/assets/article/make_tds_visuals.py [FONT_DIR]

FONT_DIR holds DejaVuSans.ttf, DejaVuSans-Bold.ttf and DejaVuSansMono.ttf (default: the current directory).
Writes tds-1-workflow.png ... tds-6-output-folder.png next to this script.
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

FONTS = Path(sys.argv[1]) if len(sys.argv) > 1 else Path(".")
OUT = Path(__file__).parent
S = 2                                                   # draw at 2x, save at 1x
NAVY, BLUE, ICE, LINE, TXT, MUTED = "#0B2545", "#3A86C8", "#EAF4FF", "#6FA8DC", "#102A43", "#486581"
ORANGE, ORANGE_ICE, WHITE = "#D9731A", "#FFF1E3", "#FFFFFF"
CODE_BG = "#F4F7FA"


def font(kind, size):
    name = {"b": "DejaVuSans-Bold.ttf", "r": "DejaVuSans.ttf", "m": "DejaVuSansMono.ttf", "mb": "DejaVuSansMono-Bold.ttf"}[kind]
    return ImageFont.truetype(str(FONTS / name), size * S)


class Canvas:
    def __init__(self, w, h):
        self.w, self.h = w, h
        self.img = Image.new("RGB", (w * S, h * S), "white")
        self.d = ImageDraw.Draw(self.img)

    def box(self, x, y, w, h, fill=ICE, outline=BLUE, width=3, radius=16):
        self.d.rounded_rectangle((x * S, y * S, (x + w) * S, (y + h) * S), radius=radius * S, fill=fill, outline=outline, width=width * S)

    def text(self, x, y, s, kind="r", size=20, fill=TXT, anchor="la"):
        self.d.text((x * S, y * S), s, font=font(kind, size), fill=fill, anchor=anchor)

    def lines(self, x, y, rows, kind="r", size=18, fill=TXT, gap=6, anchor="la"):
        for i, row in enumerate(rows):
            self.text(x, y + i * (size + gap), row, kind, size, fill, anchor)

    def arrow(self, x1, y1, x2, y2, color=NAVY, width=4, head=14):
        d, s = self.d, S
        d.line((x1 * s, y1 * s, x2 * s, y2 * s), fill=color, width=width * s)
        if abs(x2 - x1) >= abs(y2 - y1):
            sign = 1 if x2 > x1 else -1
            d.polygon([(x2 * s, y2 * s), ((x2 - sign * head * 1.3) * s, (y2 - head * 0.7) * s), ((x2 - sign * head * 1.3) * s, (y2 + head * 0.7) * s)], fill=color)
        else:
            sign = 1 if y2 > y1 else -1
            d.polygon([(x2 * s, y2 * s), ((x2 - head * 0.7) * s, (y2 - sign * head * 1.3) * s), ((x2 + head * 0.7) * s, (y2 - sign * head * 1.3) * s)], fill=color)

    def elbow(self, x1, y1, xm, y2, x2, color=NAVY, width=4, head=14):
        """Horizontal from (x1, y1) to xm, vertical to y2, then an arrow to (x2, y2)."""
        self.d.line((x1 * S, y1 * S, xm * S, y1 * S), fill=color, width=width * S)
        self.d.line((xm * S, y1 * S, xm * S, y2 * S), fill=color, width=width * S)
        self.arrow(xm, y2, x2, y2, color, width, head)

    def save(self, name):
        path = OUT / name
        self.img.resize((self.w, self.h), Image.LANCZOS).save(path, optimize=True)
        print("wrote", path)


# ---------------------------------------------------------------------------------------------- 1. workflow
def workflow():
    c = Canvas(1400, 560)
    c.text(700, 22, "From SQL files to Iceberg curated tables, in four steps", "b", 28, NAVY, "ma")
    steps = [
        ("1", "Detect", ["Find the SQL in .sql, .py,", ".ipynb files", "Which dialect? Which tables", "are read, which are created?"], "table map", ICE, NAVY, BLUE),
        ("2", "Analyze", ["Where will Spark differ from", "your database?", "converted / review /", "unsupported, with reasons"], "status report", ICE, NAVY, BLUE),
        ("3", "Convert", ["Rewrite what differs", "Simple: Spark SQL", "Complex: DataFrame steps", "One script per SQL file"], "PySpark scripts", NAVY, WHITE, NAVY),
        ("4", "Validate", ["Run the original SELECTs on", "the source; compare with the", "Iceberg tables: rows, non-null", "counts, column sums"], "PASS / differences", BLUE, WHITE, BLUE),
    ]
    x0, w, gap, y0, h = 30, 295, 50, 90, 330
    for i, (n, title, rows, chip, fill, fg, outline) in enumerate(steps):
        x = x0 + i * (w + gap)
        c.box(x, y0, w, h, fill=fill, outline=outline)
        c.d.ellipse(((x + 22) * S, (y0 + 22) * S, (x + 62) * S, (y0 + 62) * S), fill=WHITE if fill != ICE else NAVY)
        c.text(x + 42, y0 + 42, n, "b", 24, NAVY if fill != ICE else WHITE, "mm")
        c.text(x + 80, y0 + 42, title, "b", 28, fg, "lm")
        c.lines(x + 24, y0 + 90, rows, "r", 18, fg if fill != ICE else TXT, gap=10)
        c.box(x + 24, y0 + h - 70, w - 48, 46, fill=WHITE, outline=WHITE if fill != ICE else LINE, width=2, radius=12)
        c.text(x + w // 2, y0 + h - 47, chip, "b", 18, NAVY, "mm")
        if i < 3:
            c.arrow(x + w + 4, y0 + h // 2, x + w + gap - 4, y0 + h // 2)
    c.box(30, 450, 1340, 90, fill=ICE, outline=LINE, width=2, radius=14)
    c.text(60, 480, "SQL files", "b", 22, NAVY, "lm")
    c.text(60, 514, "Snowflake, Postgres, MySQL, SQL Server, Oracle", "r", 16, MUTED, "lm")
    c.arrow(560, 495, 660, 495)
    c.text(700, 480, "Iceberg curated tables", "b", 22, NAVY, "lm")
    c.text(700, 514, "readable by Spark, Trino, Snowflake, Athena, BigQuery", "r", 16, MUTED, "lm")
    c.save("tds-1-workflow.png")


# ---------------------------------------------------------------------------------------------- 2. silent differences
def silent_differences():
    c = Canvas(1400, 560)
    c.text(700, 22, "No error, different answer: the same query on both engines", "b", 28, NAVY, "ma")
    panels = [
        ("Sum of the p90 amounts of four order statuses", "PERCENTILE_CONT(0.9) WITHIN GROUP (ORDER BY amount)", 1067.42, 1088.48, "naive translation: percentile_approx", 1000),
        ("Sum of days between consecutive orders", "DATEDIFF('day', previous_order_ts, order_ts)", 342, 301, "naive translation: elapsed 24-hour periods", 250),
    ]
    for i, (title, sql, snow, spark, note, base) in enumerate(panels):
        x = 40 + i * 690
        c.box(x, 80, 640, 400, fill=WHITE, outline=LINE, width=2)
        c.text(x + 24, 104, title, "b", 22, NAVY)
        c.text(x + 24, 140, sql, "m", 14, MUTED)
        top = max(snow, spark)
        scale_max = top * 1.12
        for j, (label, val, color) in enumerate([("Snowflake", snow, BLUE), ("Spark (naive)", spark, ORANGE)]):
            bx = x + 90 + j * 260
            bh = int(210 * (val - base * 0.0) / scale_max)
            by = 440 - bh
            c.d.rectangle((bx * S, by * S, (bx + 150) * S, 440 * S), fill=color)
            c.text(bx + 75, by - 10, f"{val:,.2f}" if isinstance(val, float) else f"{val}", "b", 24, color, "ms")
            c.text(bx + 75, 458, label, "r", 18, TXT, "mt")
        c.d.line(((x + 60) * S, 440 * S, (x + 600) * S, 440 * S), fill=MUTED, width=2 * S)
        c.text(x + 24, 182, note, "r", 16, ORANGE)
    c.box(40, 495, 1320, 48, fill=ORANGE_ICE, outline=ORANGE, width=2, radius=12)
    c.text(700, 519, "Both jobs succeed. Only a comparison against the source shows the difference.", "b", 19, TXT, "mm")
    c.save("tds-2-silent-differences.png")


# ---------------------------------------------------------------------------------------------- 3. decision flow
def decision():
    c = Canvas(1400, 640)
    c.text(700, 20, "Spark SQL or DataFrames? The converter decides per statement", "b", 28, NAVY, "ma")
    c.box(40, 280, 200, 90, fill=NAVY, outline=NAVY)
    c.text(140, 310, "One SQL", "b", 20, WHITE, "mm")
    c.text(140, 340, "statement", "b", 20, WHITE, "mm")
    c.arrow(240, 325, 300, 325)
    c.box(300, 235, 330, 180)
    c.text(465, 262, "Is it simple?", "b", 22, NAVY, "mm")
    c.lines(465, 295, ["one SELECT, at most one join,", "no CTE, subquery, window", "function, set operation, pivot"], "r", 16, TXT, gap=6, anchor="ma")
    c.elbow(630, 290, 685, 150, 740, BLUE)
    c.text(660, 220, "yes", "b", 18, BLUE, "mm")
    c.elbow(630, 360, 685, 480, 740, ORANGE)
    c.text(660, 420, "no", "b", 18, ORANGE, "mm")
    c.box(740, 90, 300, 120, fill=ICE)
    c.text(890, 122, "Spark SQL", "b", 24, NAVY, "mm")
    c.text(890, 160, 'spark.sql("""CREATE TABLE ...', "m", 13, MUTED, "mm")
    c.text(890, 182, 'AS SELECT ...""")', "m", 13, MUTED, "mm")
    c.box(740, 400, 300, 160, fill=NAVY, outline=NAVY)
    c.text(890, 432, "One DataFrame per CTE", "b", 19, WHITE, "mm")
    c.lines(890, 465, ["join, where, groupBy().agg(),", "select, union, orderBy,", "pivot, explode"], "r", 16, WHITE, gap=6, anchor="ma")
    c.arrow(1040, 480, 1110, 480)
    c.box(1110, 400, 250, 160, fill=ORANGE_ICE, outline=ORANGE)
    c.text(1235, 432, "A step the builder", "b", 18, NAVY, "mm")
    c.text(1235, 456, "cannot express?", "b", 18, NAVY, "mm")
    c.lines(1235, 490, ["recursive CTE, ROLLUP,", "window over an aggregate"], "r", 15, TXT, gap=4, anchor="ma")
    c.text(1235, 588, "only that step runs as spark.sql", "b", 16, ORANGE, "mm")
    c.arrow(1040, 150, 1110, 150)
    c.box(1110, 90, 250, 120, fill=BLUE, outline=BLUE)
    c.text(1235, 126, "writeTo(...)", "b", 24, WHITE, "mm")
    c.text(1235, 160, "Iceberg table", "b", 20, WHITE, "mm")
    c.arrow(1235, 400, 1235, 215, color=NAVY)
    c.text(1252, 300, "always completed", "r", 16, MUTED, "lm")
    c.save("tds-3-decision.png")


# ---------------------------------------------------------------------------------------------- 4. CTE -> DataFrames
def cte_mapping():
    c = Canvas(1400, 770)
    c.text(700, 20, "One SQL file becomes named, testable steps", "b", 28, NAVY, "ma")
    # left: SQL
    c.box(30, 80, 560, 660, fill=CODE_BG, outline=LINE, width=2)
    c.text(50, 98, "curated_orders.sql", "b", 18, NAVY)
    sql_a = ["WITH order_totals AS (", "  SELECT o.order_id, o.customer_id,", "         o.order_ts,", "         SUM(i.qty * i.price) AS items_value,", "         COUNT(*) AS lines",
             "  FROM orders o", "  JOIN order_items i", "       ON i.order_id = o.order_id", "  WHERE o.status <> 'cancelled'", "  GROUP BY o.order_id, o.customer_id,", "           o.order_ts", ")"]
    sql_b = ["SELECT c.customer_id, c.name, c.country,", "       t.order_id,", "       DATE_TRUNC('month', t.order_ts)", "         AS order_month,", "       t.items_value, t.lines,",
             "       RANK() OVER (PARTITION BY c.country", "         ORDER BY t.items_value DESC)", "         AS country_rank", "FROM order_totals t", "JOIN customers c", "  ON c.customer_id = t.customer_id;"]
    c.box(44, 130, 532, 12 * 22 + 24, fill=ICE, outline=BLUE, width=2, radius=10)
    c.lines(58, 142, sql_a, "m", 15, TXT, gap=7)
    c.box(44, 130 + 12 * 22 + 40, 532, 12 * 22 + 24, fill=WHITE, outline=NAVY, width=2, radius=10)
    c.lines(58, 130 + 12 * 22 + 52, sql_b, "m", 15, TXT, gap=7)
    # right: PySpark
    c.box(700, 80, 670, 660, fill=WHITE, outline=LINE, width=2)
    c.text(720, 98, "curated_orders_pyspark.py", "b", 18, NAVY)
    py_a = ["# CTE order_totals: DataFrame API", "df_order_totals = (", "  spark.table('lake.db.orders').alias('o')", "  .join(<order_items>.alias('i'), <on>, 'inner')",
            "  .where(F.expr(\"o.status <> 'cancelled'\"))", "  .groupBy(<order_id, customer_id, order_ts>)", "  .agg(SUM(i.qty * i.price), COUNT(*))", ")"]
    py_b = ["# final SELECT: DataFrame API", "result = (", "  df_order_totals.alias('t')", "  .join(<customers>.alias('c'), <on>, 'inner')",
            "  .select(<columns>, <month bucket>,", "          <RANK() OVER (...)>)", ")"]
    py_c = ["result.writeTo('lake.db.curated_orders')", "  .using('iceberg').create()"]
    c.box(714, 130, 642, 8 * 22 + 24, fill=ICE, outline=BLUE, width=2, radius=10)
    c.lines(728, 142, py_a, "m", 15, TXT, gap=7)
    c.box(714, 130 + 8 * 22 + 40, 642, 7 * 22 + 24, fill=WHITE, outline=NAVY, width=2, radius=10)
    c.lines(728, 130 + 8 * 22 + 52, py_b, "m", 15, TXT, gap=7)
    c.box(714, 130 + 8 * 22 + 40 + 7 * 22 + 40, 642, 2 * 22 + 40, fill=BLUE, outline=BLUE, radius=10)
    c.lines(728, 130 + 8 * 22 + 40 + 7 * 22 + 56, py_c, "mb", 15, WHITE, gap=7)
    # arrows between panels
    c.elbow(576, 130 + 6 * 22, 640, 130 + 4 * 22, 714, BLUE)
    c.elbow(576, 130 + 12 * 22 + 40 + 6 * 22, 650, 130 + 8 * 22 + 40 + 3 * 22, 714, NAVY)
    c.arrow(1035, 130 + 8 * 22 + 40 + 7 * 22 + 24, 1035, 130 + 8 * 22 + 40 + 7 * 22 + 38, color=NAVY, head=10)
    c.lines(730, 668, ["Each step is a named DataFrame: inspect it, cache it, test it.", "<angle brackets> shorten the arguments the tool really generates."], "r", 15, MUTED, gap=8)
    c.save("tds-4-cte-to-dataframes.png")


# ---------------------------------------------------------------------------------------------- 5. validation results
def validation():
    c = Canvas(1400, 520)
    c.text(700, 22, "Checks matched against the source database (Snowflake)", "b", 28, NAVY, "ma")
    rows = [("10 basic queries", "joins, aggregates, windows, QUALIFY", 84, 84), ("8 INSERT statements", "VALUES, CASE, UNION, CTE, regex", 32, 32),
            ("10 complex queries", "cohorts, windows, PIVOT, FLATTEN, recursion", 99, 100)]
    x0, w = 470, 830
    scale = w / 100
    for i, (name, sub, ok, total) in enumerate(rows):
        y = 100 + i * 105
        c.text(40, y + 8, name, "b", 22, NAVY)
        c.text(40, y + 40, sub, "r", 14, MUTED)
        c.d.rectangle((x0 * S, y * S, (x0 + total * scale) * S, (y + 56) * S), fill=ICE, outline=LINE, width=2 * S)
        c.d.rectangle((x0 * S, y * S, (x0 + ok * scale) * S, (y + 56) * S), fill=BLUE)
        if ok < total:
            c.d.rectangle(((x0 + ok * scale) * S, y * S, (x0 + total * scale) * S, (y + 56) * S), fill=ORANGE)
        c.text(x0 + ok * scale - 14, y + 28, f"{ok} of {total}", "b", 22, WHITE, "rm")
        if ok == total:
            c.text(x0 + total * scale + 16, y + 28, "all match", "b", 18, BLUE, "lm")
    c.box(40, 420, 1320, 80, fill=ORANGE_ICE, outline=ORANGE, width=2, radius=14)
    c.text(700, 444, "The one difference: a windowed AVG, in the last digits (totals 15229.52982 and 15229.529997).", "r", 17, TXT, "mm")
    c.text(700, 474, "Snowflake returned 5 decimals, Spark 6. Documented in known-issues; not a conversion error.", "r", 17, TXT, "mm")
    c.save("tds-5-validation.png")


# ---------------------------------------------------------------------------------------------- 6. output folder
def output_folder():
    c = Canvas(1400, 600)
    c.text(700, 20, "What the converter writes: a folder that explains itself", "b", 28, NAVY, "ma")
    c.box(30, 80, 560, 490, fill=CODE_BG, outline=LINE, width=2)
    tree = [("out/", "b", NAVY), ("  README.md", "mb", NAVY), ("  run_all.py", "mb", NAVY), ("  scripts/", "b", NAVY), ("    curated_orders_pyspark.py", "m", TXT),
            ("    daily_revenue_pyspark.py", "m", TXT), ("  reports/", "b", NAVY), ("    conversion_report.md", "m", TXT), ("    conversion_report.json", "m", TXT), ("    validate.json", "mb", NAVY)]
    ys = {}
    for i, (row, kind, color) in enumerate(tree):
        y = 110 + i * 42
        ys[row.strip()] = y
        c.text(56, y, row, kind, 19, color, "lm")
    notes = [("README.md", "Start here: scripts in run order, what to double-check", BLUE),
             ("run_all.py", "Runs every script in dependency order", BLUE),
             ("curated_orders_pyspark.py", "Copies customers, orders, order_items; builds curated_orders", NAVY),
             ("daily_revenue_pyspark.py", "Copies orders; builds daily_revenue", NAVY),
             ("conversion_report.md", "Every statement: how it was converted, what to check", MUTED),
             ("validate.json", "Ready-made check: original SELECTs vs the Iceberg tables", ORANGE)]
    for j, (key, note, color) in enumerate(notes):
        y = ys[key]
        c.box(660, y - 17, 700, 34, fill=WHITE, outline=color, width=2, radius=8)
        c.text(680, y, note, "r", 17, TXT, "lm")
        c.arrow(640, y, 660, y, color=color, width=3, head=10)
        c.d.line((520 * S, y * S, 640 * S, y * S), fill=color, width=2 * S)
    c.save("tds-6-output-folder.png")


if __name__ == "__main__":
    workflow()
    silent_differences()
    decision()
    cte_mapping()
    validation()
    output_folder()
