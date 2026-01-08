with src as (
  select *
  from {{ source('bronze', 'products') }}
  {{ latest_snapshot_where(source('bronze', 'products')) }}
)

select
  cast(id as int64) as product_id,
  cast(name as string) as product_name,
  cast(category as string) as category,
  cast(price as numeric) as price,
  cast(ingest_ts as timestamp) as ingest_time,
from src