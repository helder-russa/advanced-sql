-- Identify anomalous trips
-- tag::anomaly_detection[]
WITH losses AS (
  SELECT
    *,
    mean_squared_error AS reconstruction_error
  FROM ML.RECONSTRUCTION_LOSS(
    MODEL `oreilly.trip_anomaly_detector`,
    (
      SELECT
        tripduration,
        start_hour,
        day_of_week,
        distance_m
      FROM `oreilly.usertype_classification_features`
      LIMIT 10000
    )
  )
),
p95 AS (
  SELECT APPROX_QUANTILES(reconstruction_error, 100)[OFFSET(95)] AS thresh
  FROM losses
)
SELECT
  tripduration,
  start_hour,
  distance_m,
  reconstruction_error,
  CASE 
    WHEN reconstruction_error > p95.thresh THEN 'ANOMALY' 
    ELSE 'NORMAL' 
  END AS anomaly_flag
FROM losses, p95
ORDER BY reconstruction_error DESC
LIMIT 20;

-- end::anomaly_detection[]

