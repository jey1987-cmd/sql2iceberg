"""Regenerate docs/assets/logo.png (the banner used in the README and on PyPI).

    python docs/assets/make_logo.py [path/to/DejaVuSans-Bold.ttf path/to/DejaVuSans.ttf]

Needs Pillow. Uses DejaVu Sans (free license); matplotlib ships it in mpl-data/fonts/ttf.
"""
import sys
from pathlib import Path

from PIL import Image, ImageDraw, ImageFont

BOLD = sys.argv[1] if len(sys.argv) > 2 else "DejaVuSans-Bold.ttf"
REGULAR = sys.argv[2] if len(sys.argv) > 2 else "DejaVuSans.ttf"
W, H = 2560, 720                      # drawn at 2x, saved at half size for smooth edges
NAVY, WATER, ICE, ICE_SHADE, SUBMERGED = "#0B2545", "#13315C", "#EAF4FF", "#BCD9F5", "#3A86C8"
ACCENT, TAGLINE = "#5EC8F2", "#9DB8D6"

img = Image.new("RGBA", (W, H), (0, 0, 0, 0))
d = ImageDraw.Draw(img)
d.rounded_rectangle((0, 0, W - 1, H - 1), radius=90, fill=NAVY)
d.rectangle((0, 430, 620, H - 1), fill=WATER)                                         # water
d.rounded_rectangle((0, 430, 620, H - 1), radius=90, fill=WATER, corners=(False, False, False, True))
d.polygon([(110, 430), (480, 430), (530, 530), (410, 650), (240, 670), (125, 570)], fill=SUBMERGED)
d.polygon([(150, 430), (230, 262), (290, 305), (345, 150), (435, 330), (485, 430)], fill=ICE)
d.polygon([(345, 150), (435, 330), (485, 430), (365, 430)], fill=ICE_SHADE)
d.line((0, 430, 620, 430), fill="#6FA8DC", width=6)

big = ImageFont.truetype(BOLD, 215)
small = ImageFont.truetype(REGULAR, 76)
x = 700
for text, color in (("sql", "#FFFFFF"), ("2", ACCENT), ("iceberg", "#FFFFFF")):
    d.text((x, 150), text, font=big, fill=color)
    x += int(d.textlength(text, font=big))
d.text((706, 440), "SQL  \u2192  PySpark  \u2192  Apache Iceberg", font=small, fill=TAGLINE)

out = Path(__file__).with_name("logo.png")
img.resize((W // 2, H // 2), Image.LANCZOS).save(out, optimize=True)
print("wrote", out)
