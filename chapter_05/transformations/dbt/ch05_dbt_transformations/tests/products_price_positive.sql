select *
from {{ ref('stg_products') }}
where price is null or price <= 0