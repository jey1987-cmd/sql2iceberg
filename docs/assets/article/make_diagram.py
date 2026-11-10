"""Render the pipeline diagram used in the how-to article.

    python docs/assets/article/make_diagram.py [DejaVuSans-Bold.ttf DejaVuSans.ttf]
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

BOLD, REG = sys.argv[1:3] if len(sys.argv) > 2 else ["DejaVuSans-Bold.ttf", "DejaVuSans.ttf"]
S, W, H = 2, 1400, 560
NAVY, BLUE, ICE, LINE, TXT = "#0B2545", "#3A86C8", "#EAF4FF", "#6FA8DC", "#102A43"
img = Image.new("RGB", (W * S, H * S), "white")
d = ImageDraw.Draw(img)
fb, fr, fs = (ImageFont.truetype(BOLD, 24 * S), ImageFont.truetype(REG, 20 * S), ImageFont.truetype(REG, 17 * S))


def box(x, y, w, h, title, sub="", fill=ICE, color=TXT, outline=BLUE):
    x, y, w, h = x * S, y * S, w * S, h * S
    d.rounded_rectangle((x, y, x + w, y + h), radius=18 * S, fill=fill, outline=outline, width=3 * S)
    ty = y + (h - (30 * S + (26 * S if sub else 0))) // 2
    d.text((x + w // 2, ty), title, font=fb, fill=color, anchor="mt")
    if sub:
        d.text((x + w // 2, ty + 36 * S), sub, font=fs, fill=color, anchor="mt")


def arrow(x1, y1, x2, y2, label=""):
    x1, y1, x2, y2 = x1 * S, y1 * S, x2 * S, y2 * S
    d.line((x1, y1, x2 - 12 * S, y2), fill=NAVY, width=4 * S)
    d.polygon([(x2, y2), (x2 - 18 * S, y2 - 10 * S), (x2 - 18 * S, y2 + 10 * S)], fill=NAVY)
    if label:
        d.text(((x1 + x2) // 2, y1 - 26 * S), label, font=fs, fill="#486581", anchor="mt")


box(20, 150, 230, 130, "Snowflake", "source tables")
box(330, 150, 250, 130, "PySpark job", "generated", fill=NAVY, color="white", outline=NAVY)
box(660, 150, 250, 130, "Apache Iceberg", "files on S3", fill=BLUE, color="white", outline=BLUE)
arrow(250, 215, 330, 215, "JDBC")
arrow(580, 215, 660, 215, "write")
box(1000, 40, 370, 110, "Snowflake", "external Iceberg tables")
box(1000, 175, 370, 110, "AWS", "Glue catalog + Athena")
box(1000, 310, 370, 110, "BigQuery", "BigLake (copy on GCS)")
d.line((910 * S, 215 * S, 955 * S, 215 * S), fill=NAVY, width=4 * S)          # trunk with horizontal branches
d.line((955 * S, 95 * S, 955 * S, 365 * S), fill=NAVY, width=4 * S)
for y in (95, 230, 365):
    arrow(955, y, 1000, y)
d.rounded_rectangle((20 * S, 470 * S, 1370 * S, 530 * S), radius=14 * S, fill=ICE, outline=LINE, width=2 * S)
d.text((695 * S, 500 * S), "validate: row counts  \u00b7  non-null counts  \u00b7  column sums, on every platform", font=fr, fill=TXT, anchor="mm")
out = Path(__file__).with_name("pipeline.png")
img.resize((W, H), Image.LANCZOS).save(out, optimize=True)
print("wrote", out)
