-- Made-up examples only.
SELECT id, ts, amt FROM orders QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY ts DESC) = 1;
SELECT e.id, f.value:sku::string AS sku FROM events e, LATERAL FLATTEN(input => e.payload:items) f;
SELECT k, LISTAGG(v, ',') WITHIN GROUP (ORDER BY v) AS vs FROM t GROUP BY k;
SELECT DATEADD(day, 7, ts) AS d7 FROM orders;
