-- Skew: compare today's serving data to the model's training stats
-- tag::skew[]
SELECT *
FROM ML.VALIDATE_DATA_SKEW(
  MODEL `oreilly.usertype_forest`,
  (
    SELECT
      tripduration,
      distance_m,
      start_station_name,
      start_hour,
    FROM `oreilly.usertype_classification_features`
    WHERE DATE(starttime) = CURRENT_DATE()
      AND tripduration IS NOT NULL
      AND distance_m   IS NOT NULL
      AND start_station_name IS NOT NULL
  ),
  STRUCT(0.2 AS categorical_default_threshold)
);

-- end::skew[]

