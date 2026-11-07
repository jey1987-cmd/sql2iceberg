"""Write small made-up seed CSVs (deterministic) for the synthetic corpus."""
import csv
import random
import sys
from datetime import datetime, timedelta
from pathlib import Path

out = Path(sys.argv[1] if len(sys.argv) > 1 else "seed"); out.mkdir(parents=True, exist_ok=True)
rnd = random.Random(7); base = datetime(2026, 1, 1)
def w(name, header, rows):
    with open(out / f"{name}.csv", "w", newline="") as fh:
        c = csv.writer(fh); c.writerow(header); c.writerows(rows)
names = ["Ann Lee", "Bob Ray", "Cy Dunn", "Di Fox", "Al Gray"]
w("customers", ["customer_id","name","country","signup_ts"],
  [(i, names[i % 5], "US" if i % 2 else "CA", base + timedelta(days=i)) for i in range(1, 21)])
orders = [(i, rnd.randint(1, 20), base + timedelta(hours=7 * i), round(rnd.uniform(5, 300), 2),
           rnd.choice(["open", "shipped", "closed", "cancelled"]), rnd.choice(["", "SAVE10", "20260105"])) for i in range(1, 101)]
w("orders", ["order_id","customer_id","order_ts","amount","status","coupon"], orders)
w("order_items", ["order_id","sku","qty","price"],
  [(o[0], f"SKU{rnd.randint(1, 9)}", rnd.randint(0, 4), round(rnd.uniform(1, 50), 2)) for o in orders])
