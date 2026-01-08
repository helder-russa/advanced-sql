with src as (
  select *
  from {{ source('bronze', 'orders') }}
  {{ latest_snapshot_where(source('bronze', 'orders')) }}
)

select
  cast(id as int64) as order_id,
  cast(customer_id as int64) as customer_id,
  cast(product_id as int64) as product_id,
  cast(quantity as int64) as quantity,
  cast(unit_price as numeric) as unit_price,
  cast(total_amount as numeric) as total_amount,
  cast(created_at as timestamp) as created_at,
  cast(ingest_ts as timestamp) as ingest_time,
from src