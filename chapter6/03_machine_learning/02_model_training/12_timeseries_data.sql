-- Prepare daily trip volume data
-- tag::timeseries_data[]
CREATE OR REPLACE TABLE `oreilly.daily_trip_volume` AS
SELECT 
  DATE(starttime) as trip_date,
  COUNT(*) as daily_trips,
  AVG(tripduration) as avg_duration,
  COUNT(DISTINCT start_station_name) as active_stations,
  MAX(CASE WHEN EXTRACT(DAYOFWEEK FROM starttime) IN (1,7) THEN 1 ELSE 0 END) as is_weekend
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE starttime >= '2017-01-01'
  AND starttime < '2018-01-01'
GROUP BY DATE(starttime)
ORDER BY trip_date;
-- end::timeseries_data[]

