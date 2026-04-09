-- Outlier detection using percentiles
-- tag::outlier_percentile[]
WITH percentiles AS (
  SELECT
    APPROX_QUANTILES(tripduration, 100)[OFFSET(1)] AS p1,
    APPROX_QUANTILES(tripduration, 100)[OFFSET(99)] AS p99
  FROM
    `bigquery-public-data.new_york_citibike.citibike_trips`
)
SELECT
  *
FROM
  `bigquery-public-data.new_york_citibike.citibike_trips`, percentiles
WHERE
  tripduration < p1 OR tripduration > p99
LIMIT 20;
-- end::outlier_percentile[]

