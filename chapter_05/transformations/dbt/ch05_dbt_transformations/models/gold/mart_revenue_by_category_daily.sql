select
  date(o.created_at) as order_date,
  p.category,
  count(*) as orders,
  sum(o.total_amount) as revenue
from {{ ref('fct_orders') }} o
join {{ ref('dim_products') }} p
  on o.product_id = p.product_id
group by 1,2
