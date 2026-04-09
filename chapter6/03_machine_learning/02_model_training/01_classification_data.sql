-- Prepare classification training data
-- tag::classification_data[]
CREATE OR REPLACE TABLE `oreilly.usertype_classification_features` AS
SELECT
  usertype,
  tripduration,
  starttime,
  EXTRACT(HOUR FROM starttime) AS start_hour,
  EXTRACT(DAYOFWEEK FROM starttime) AS day_of_week,
  start_station_name,
  ST_DISTANCE(
    ST_GEOGPOINT(start_station_longitude, start_station_latitude),
    ST_GEOGPOINT(end_station_longitude, end_station_latitude)
  ) AS distance_m
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE usertype IS NOT NULL
  AND tripduration IS NOT NULL
  AND tripduration BETWEEN 60 AND 7200
  AND start_station_name IS NOT NULL
  AND start_station_latitude IS NOT NULL 
  AND end_station_latitude IS NOT NULL;
-- end::classification_data[]

