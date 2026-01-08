select *
from {{ ref('fct_orders') }}
where quantity is null or quantity <= 0