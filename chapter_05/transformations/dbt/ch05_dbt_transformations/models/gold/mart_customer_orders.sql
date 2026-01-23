select
  o.customer_id,
  count(*) as orders,
  sum(o.total_amount) as revenue,
  max(o.created_at) as last_order_at
from {{ ref('fct_orders') }} o
group by 1
