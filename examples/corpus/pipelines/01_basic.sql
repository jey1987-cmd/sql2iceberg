-- basic expressions
CREATE TABLE daily_sales AS SELECT DATE_TRUNC('day', order_ts) AS d, SUM(amount) AS total, COUNT(*) AS n FROM orders GROUP BY 1;
SELECT order_id, IFF(amount > 100, 'big', 'small') AS size FROM orders;
SELECT customer_id, ZEROIFNULL(amount) AS amt, NVL(coupon, 'none') AS coupon FROM orders;
SELECT order_id, NVL2(coupon, 'used', 'unused') AS coupon_state FROM orders;
SELECT order_id, amount / NULLIF(qty, 0) AS unit FROM orders o JOIN order_items i USING (order_id);
SELECT order_id, TO_VARCHAR(order_ts, 'YYYY-MM-DD') AS day_s FROM orders;
SELECT customer_id, UPPER(name) AS n, SPLIT_PART(name, ' ', 1) AS first FROM customers;
SELECT order_id FROM orders WHERE status ILIKE 'ship%';
SELECT order_id, TRY_TO_NUMBER(coupon) AS cn FROM orders;
