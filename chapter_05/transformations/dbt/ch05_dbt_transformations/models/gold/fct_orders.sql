select
  order_id,
  customer_id,
  product_id,
  quantity,
  unit_price,
  total_amount,
  created_at
from {{ ref('stg_orders') }}
