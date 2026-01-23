select
  date(created_at) as order_date,
  count(*) as orders,
  sum(total_amount) as total_revenue
from {{ ref('fct_orders') }}
group by 1