-- strings and regex
SELECT name FROM customers WHERE REGEXP_LIKE(name, 'A.*');
SELECT REGEXP_REPLACE(name, '[^a-z]', '') AS clean FROM customers;
SELECT customer_id, CONCAT_WS('-', country, name) AS key FROM customers;
SELECT customer_id, CHARINDEX('a', name) AS pos FROM customers;
SELECT customer_id, LPAD(TO_VARCHAR(customer_id), 8, '0') AS padded FROM customers;
