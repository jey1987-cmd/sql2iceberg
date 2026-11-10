"""Render the article's two tables as PNGs (Medium does not support tables).

    python docs/assets/article/make_tables.py [DejaVuSans-Bold.ttf DejaVuSans.ttf DejaVuSansMono.ttf]
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

BOLD, REG, MONO = (sys.argv[1:4] if len(sys.argv) > 3 else
                   ["DejaVuSans-Bold.ttf", "DejaVuSans.ttf", "DejaVuSansMono.ttf"])
S, W = 2, 1400                                  # draw at 2x, save at final width W
NAVY, ZEBRA, GRID = "#0B2545", "#F3F7FB", "#C9D6E4"
GREEN, RED = "#DDF3E4", "#FBE0E0"


def render(name, headers, rows, widths, fills=None, mono_cols=(), legend=None):
    pad, fs = 14 * S, 22 * S
    f_b, f_r, f_m = (ImageFont.truetype(BOLD, fs), ImageFont.truetype(REG, fs), ImageFont.truetype(MONO, fs - 2 * S))
    cols = [int(w * W * S) for w in widths]
    probe = ImageDraw.Draw(Image.new("RGB", (10, 10)))

    def wrap(text, font, width):
        lines, cur = [], ""
        for word in text.split(" "):
            t = (cur + " " + word).strip()
            if probe.textlength(t, font=font) <= width - 2 * pad or not cur:
                cur = t
            else:
                lines.append(cur)
                cur = word
        return lines + [cur]

    lh = fs + 8 * S
    grid = [[wrap(c, f_b if r < 0 else (f_m if i in mono_cols else f_r), cols[i]) for i, c in enumerate(row)]
            for r, row in enumerate([headers] + rows, start=-1)]
    heights = [max(len(c) for c in row) * lh + 2 * pad for row in grid]
    total_h = sum(heights) + (int(lh * 1.6) if legend else 0)
    img = Image.new("RGB", (sum(cols), total_h), "white")
    d = ImageDraw.Draw(img)
    y = 0
    for r, row in enumerate(grid):
        x = 0
        for i, lines in enumerate(row):
            if r == 0:
                fill, font, color = NAVY, f_b, "white"
            else:
                fill = (fills or {}).get((r - 1, i), ZEBRA if r % 2 == 0 else "white")
                font, color = (f_m if i in mono_cols else f_r), "#102A43"
            d.rectangle((x, y, x + cols[i], y + heights[r]), fill=fill, outline=GRID, width=S)
            for k, line in enumerate(lines):
                d.text((x + pad, y + pad + k * lh), line, font=font, fill=color)
            x += cols[i]
        y += heights[r]
    if legend:
        d.text((pad, y + lh // 2), legend, font=f_r, fill="#486581")
    out = Path(__file__).with_name(name)
    img.resize((W, round(img.height * W / img.width)), Image.LANCZOS).save(out, optimize=True)
    print("wrote", out)


# table 1: Snowflake vs Spark. Green = same as Snowflake, red = differs
rows1 = [
    ["DATEADD(day, 7, '2026-01-05 10:30:00'::TIMESTAMP_NTZ)", "2026-01-12 10:30:00", "2026-01-12 (a date: time lost)", "2026-01-12 (time lost)"],
    ["1/0", "error: division by zero", "NULL", "error"],
    ["TO_DATE('2026-13-45', 'YYYY-MM-DD')", "error: can't parse", "NULL", "error"],
    ["TRY_TO_DATE('2026-13-45', ...)", "NULL", "NULL", "NULL"],
    ["REGEXP_LIKE('abc123', '[0-9]+')", "FALSE (whole string must match)", "TRUE (RLIKE matches anywhere)", "TRUE"],
]
same = {(0, 2): RED, (0, 3): RED, (1, 2): RED, (1, 3): GREEN, (2, 2): RED, (2, 3): GREEN,
        (3, 2): GREEN, (3, 3): GREEN, (4, 2): RED, (4, 3): RED}
render("snowflake-vs-spark.png", ["Snowflake SQL", "Snowflake", "Spark 4.1.4, ANSI off", "Spark 4.1.4, ANSI on (default)"],
       rows1, [0.34, 0.22, 0.24, 0.20], same, mono_cols=(0,), legend="Green: same result as Snowflake.  Red: different result.")

# table 2: validation
rows2 = [
    ["customers", "20", "count, non-nulls, sum of ids"],
    ["orders", "100", "count, non-nulls, sums of ids and amount"],
    ["order_items", "100", "count, non-nulls, sums of quantity and price"],
    ["curated_orders", "75", "count, non-nulls, sums of every numeric column"],
]
render("validation-results.png", ["Table", "Rows", "What matched on all four platforms (38 checks)"], rows2,
       [0.24, 0.10, 0.66], {(i, 1): GREEN for i in range(4)} | {(i, 2): GREEN for i in range(4)}, mono_cols=(0,))
