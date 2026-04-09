-- Explain individual predictions (local attributions)
-- tag::explain_predict[]
SELECT *
FROM ML.EXPLAIN_PREDICT(
  MODEL `oreilly.taxi_tipped_lr`,
  (SELECT
     CAST(tips > 0 AS INT64) AS tipped,
     trip_seconds,
     trip_miles,
     EXTRACT(HOUR FROM trip_start_timestamp) AS pickup_hour,
     payment_type
   FROM `bigquery-public-data.chicago_taxi_trips.taxi_trips`
   WHERE trip_seconds IS NOT NULL
     AND trip_miles  IS NOT NULL
     AND tips        IS NOT NULL
     AND trip_start_timestamp IS NOT NULL
   LIMIT 50)
);
-- end::explain_predict[]

