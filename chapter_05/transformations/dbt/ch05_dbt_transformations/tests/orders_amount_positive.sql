select *
from {{ ref('fct_orders') }}
where total_amount is null or total_amount <= 0