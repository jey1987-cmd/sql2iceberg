-- date and time
SELECT order_id, DATEADD(day, 7, order_ts) AS due FROM orders;
SELECT order_id, DATEDIFF(day, order_ts, CURRENT_TIMESTAMP()) AS age FROM orders;
SELECT customer_id, TO_DATE(signup_ts) AS signup_d FROM customers;
SELECT order_id, TO_DATE(coupon, 'YYYYMMDD') AS cd FROM orders;
SELECT order_id, TRY_TO_TIMESTAMP(coupon) AS t FROM orders;
SELECT event_id, CONVERT_TIMEZONE('UTC', 'America/New_York', event_ts) AS local_ts FROM events;
SELECT LAST_DAY(order_ts) AS eom, MONTHNAME(order_ts) AS mon FROM orders;
