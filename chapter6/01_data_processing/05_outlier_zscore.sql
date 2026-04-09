-- Outlier detection using z-score
-- tag::outlier_zscore[]
WITH stats AS (
  SELECT
    AVG(tripduration) AS mean_duration,
    STDDEV(tripduration) AS stddev_duration
  FROM
    `bigquery-public-data.new_york_citibike.citibike_trips`
)
SELECT
  tripduration,
  (tripduration - mean_duration) / stddev_duration AS z_score
FROM
  `bigquery-public-data.new_york_citibike.citibike_trips`, stats
WHERE
  ABS((tripduration - mean_duration) / stddev_duration) > 3
LIMIT 20;
-- end::outlier_zscore[]

