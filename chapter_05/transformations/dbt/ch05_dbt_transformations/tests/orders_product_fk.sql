select o.*
from {{ ref('fct_orders') }} o
left join {{ ref('dim_products') }} p
  on o.product_id = p.product_id
where p.product_id is null
