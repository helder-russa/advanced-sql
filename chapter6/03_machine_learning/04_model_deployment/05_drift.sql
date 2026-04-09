-- Drift: last 7 days vs. previous 7 days
-- tag::drift[]
DECLARE run_date DATE DEFAULT CURRENT_DATE();

WITH base AS (
  SELECT
    tripduration,
    distance_m,
    start_station_name,
    start_hour,
  FROM `oreilly.usertype_classification_features`
  WHERE DATE(starttime) BETWEEN DATE_SUB(run_date, INTERVAL 14 DAY)
                              AND DATE_SUB(run_date, INTERVAL 8 DAY)
    AND tripduration IS NOT NULL AND distance_m IS NOT NULL
),
study AS (
  SELECT
    tripduration,
    distance_m,
    start_station_name,
    start_hour,
  FROM `oreilly.usertype_classification_features`
  WHERE DATE(starttime) BETWEEN DATE_SUB(run_date, INTERVAL 7 DAY)
                              AND DATE_SUB(run_date, INTERVAL 1 DAY)
    AND tripduration IS NOT NULL AND distance_m IS NOT NULL
)
SELECT *
FROM ML.VALIDATE_DATA_DRIFT(
  (SELECT * FROM base),
  (SELECT * FROM study),
  STRUCT(0.2 AS categorical_default_threshold)
);
-- end::drift[]

