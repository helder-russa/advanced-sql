-- Prepare station clustering features
-- tag::kmeans_data[]
CREATE OR REPLACE TABLE `oreilly.station_clustering_features` AS
SELECT
  start_station_name,
  COUNT(*) AS total_trips,
  AVG(tripduration) AS avg_duration,
  STDDEV(tripduration) AS stddev_duration,
  COUNT(DISTINCT EXTRACT(DATE FROM starttime)) AS active_days,
  AVG(EXTRACT(HOUR FROM starttime)) AS avg_start_hour,
  COUNT(DISTINCT usertype) AS user_types
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE start_station_name IS NOT NULL
  AND tripduration IS NOT NULL
  AND tripduration BETWEEN 60 AND 7200
GROUP BY start_station_name
HAVING COUNT(*) >= 100;  -- Filter stations with sufficient data
-- end::kmeans_data[]

