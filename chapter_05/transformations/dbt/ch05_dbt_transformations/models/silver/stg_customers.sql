with src as (
  select *
  from {{ source('bronze', 'customers') }}
  {{ latest_snapshot_where(source('bronze', 'customers')) }}
)

select
  cast(id as int64) as customer_id,
  cast(name as string) as customer_name,
  cast(email as string) as email,
  cast(country as string) as country,
  cast(ingest_ts as timestamp) as ingest_time,
from src
