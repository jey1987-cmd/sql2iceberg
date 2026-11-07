-- VARIANT, ARRAY, OBJECT
SELECT event_id, payload:user.id::NUMBER AS uid FROM events;
SELECT e.event_id, f.value:sku::STRING AS sku FROM events e, LATERAL FLATTEN(input => e.payload:items) f;
SELECT customer_id, ARRAY_SIZE(tags) AS ntags FROM customers;
SELECT customer_id, OBJECT_CONSTRUCT('id', customer_id, 'name', name) AS o FROM customers;
SELECT customer_id, ARRAY_AGG(order_id) AS ids FROM orders GROUP BY customer_id;
SELECT customer_id, LISTAGG(status, ',') WITHIN GROUP (ORDER BY order_ts) AS statuses FROM orders GROUP BY customer_id;
SELECT PARSE_JSON('{"a":1}'):a::NUMBER AS a;
