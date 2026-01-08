select o.*
from {{ ref('fct_orders') }} o
left join {{ ref('dim_customers') }} c
  on o.customer_id = c.customer_id
where c.customer_id is null
