-- Made-up schema for the synthetic corpus.
CREATE TABLE customers (customer_id NUMBER(10,0), name VARCHAR(100), country VARCHAR(2), signup_ts TIMESTAMP_NTZ, tags ARRAY, attrs VARIANT);
CREATE TABLE orders (order_id NUMBER(10,0), customer_id NUMBER(10,0), order_ts TIMESTAMP_NTZ, amount NUMBER(12,2), status VARCHAR(20), coupon VARCHAR(20));
CREATE TABLE order_items (order_id NUMBER(10,0), sku VARCHAR(20), qty NUMBER(5,0), price FLOAT);
CREATE TABLE events (event_id NUMBER(10,0), customer_id NUMBER(10,0), event_ts TIMESTAMP_TZ, payload VARIANT);
