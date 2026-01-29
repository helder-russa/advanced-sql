WITH filtered AS (
  SELECT *
  FROM {{ source('bronze', 'live_orders') }}
  WHERE DATE(created_at) = CURRENT_DATE()
    AND ingest_ts >= TIMESTAMP_SUB(TIMESTAMP(CURRENT_DATE()), INTERVAL 48 HOUR)
),
ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY ingest_ts DESC) AS row_num
  FROM filtered
)
SELECT
  CAST(id AS INT64) AS order_id,
  CAST(customer_id AS INT64) AS customer_id,
  CAST(product_id AS INT64) AS product_id,
  CAST(quantity AS INT64) AS quantity,
  CAST(unit_price AS NUMERIC) AS unit_price,
  CAST(total_amount AS NUMERIC) AS total_amount,
  CAST(created_at AS TIMESTAMP) AS created_at,
  CAST(ingest_ts AS TIMESTAMP) AS ingest_time
FROM ranked
WHERE row_num = 1