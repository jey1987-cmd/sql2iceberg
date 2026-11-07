"""Made-up example: SQL inside Python calls."""
def run(cur, name):
    cur.execute("SELECT IFF(amt > 0, 'pos', 'neg') AS s FROM orders")
    cur.execute(f"SELECT * FROM {name}")          # dynamic: reported, not converted
