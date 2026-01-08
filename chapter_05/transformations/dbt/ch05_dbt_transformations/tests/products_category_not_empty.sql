select *
from {{ ref('stg_products') }}
where category is null or trim(category) = ''