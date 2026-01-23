SELECT 
  DATE(o.created_at) AS order_date,
  COUNT(DISTINCT o.order_id) AS total_orders,
  SUM(o.total_amount)       AS total_revenue,
  SAFE_DIVIDE(SUM(o.total_amount), COUNT(DISTINCT o.order_id)) AS avg_order_value
FROM {{ ref('fct_orders') }} o
GROUP BY DATE(o.created_at)