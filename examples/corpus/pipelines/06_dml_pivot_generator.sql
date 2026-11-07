-- DML, PIVOT, generators
MERGE INTO customers c USING (SELECT customer_id, name FROM staging_customers) s ON c.customer_id = s.customer_id WHEN MATCHED THEN UPDATE SET c.name = s.name WHEN NOT MATCHED THEN INSERT (customer_id, name) VALUES (s.customer_id, s.name);
INSERT INTO daily_sales SELECT DATE_TRUNC('day', order_ts), SUM(amount), COUNT(*) FROM orders GROUP BY 1;
SELECT * FROM (SELECT customer_id, status, amount FROM orders) PIVOT (SUM(amount) FOR status IN ('open', 'shipped', 'closed'));
SELECT SEQ4() AS n FROM TABLE(GENERATOR(ROWCOUNT => 10));
DELETE FROM orders WHERE status = 'cancelled';
CREATE OR REPLACE TABLE big_customers AS SELECT c.customer_id, c.name, SUM(o.amount) AS spend FROM customers c JOIN orders o ON o.customer_id = c.customer_id GROUP BY 1, 2 HAVING SUM(o.amount) > 1000;
