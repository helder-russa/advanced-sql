{% macro latest_snapshot_where(source_relation, ingest_ts_col='ingest_ts') %}
where ({{ ingest_ts_col }}) = (
  select max({{ ingest_ts_col }})
  from {{ source_relation }}
)
{% endmacro %}