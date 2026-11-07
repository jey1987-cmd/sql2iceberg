-- window functions
SELECT order_id, customer_id, order_ts FROM orders QUALIFY ROW_NUMBER() OVER (PARTITION BY customer_id ORDER BY order_ts DESC) = 1;
SELECT order_id, SUM(amount) OVER (PARTITION BY customer_id ORDER BY order_ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running FROM orders;
SELECT order_id, LAG(amount) OVER (PARTITION BY customer_id ORDER BY order_ts) AS prev_amt FROM orders;
SELECT customer_id, RANK() OVER (ORDER BY SUM(amount) DESC) AS rk FROM orders GROUP BY customer_id;
