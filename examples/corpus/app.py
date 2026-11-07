"""Made-up app that runs Snowflake SQL."""
import snowflake.connector

conn = snowflake.connector.connect()
cur = conn.cursor()
cur.execute("INSERT INTO order_summary SELECT customer_id, COUNT(*) AS n, SUM(amount) AS total FROM orders GROUP BY 1")
cur.execute("SELECT * FROM order_summary WHERE total > 500")
cur.execute(f"SELECT * FROM {'orders'} WHERE 1=1")
