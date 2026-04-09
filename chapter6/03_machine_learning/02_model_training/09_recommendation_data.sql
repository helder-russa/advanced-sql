-- Prepare user-station interaction data
-- tag::recommendation_data[]
CREATE OR REPLACE TABLE `oreilly.user_station_interactions` AS
WITH user_trips AS (
  SELECT 
    CONCAT('user_', MOD(ABS(FARM_FINGERPRINT(
      CONCAT(CAST(bikeid AS STRING), start_station_name))), 10000)) AS user_id,
    start_station_name AS station_id,
    COUNT(*) AS trip_count
  FROM `bigquery-public-data.new_york_citibike.citibike_trips`
  WHERE start_station_name IS NOT NULL
    AND bikeid IS NOT NULL
  GROUP BY user_id, station_id
  HAVING trip_count >= 2  -- Focus on repeat usage
)
SELECT 
  user_id,
  station_id,
  LEAST(trip_count, 5) AS rating  -- Cap at 5 for rating scale
FROM user_trips;
-- end::recommendation_data[]

