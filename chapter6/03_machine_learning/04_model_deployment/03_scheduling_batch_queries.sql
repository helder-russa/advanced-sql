-- Scheduling batch predictions
-- tag::scheduling_batch_queries[]
DECLARE run_date DATE DEFAULT CURRENT_DATE();

INSERT INTO `oreilly.predictions_usertype_daily`  -- partitioned by score_date DATE
  (score_date, trip_key, predicted_label, predicted_probability)
SELECT
  run_date AS score_date,
  GENERATE_UUID() AS trip_key,
  business_decision AS predicted_label,
  predicted_probability
FROM (
  SELECT
    CASE
      WHEN subscriber_prob >= 0.40 THEN 'Subscriber'
      ELSE 'Customer'
    END AS business_decision,
    subscriber_prob AS predicted_probability
  FROM (
    SELECT
      -- pull the model's probability for the 'Subscriber' class
      (SELECT prob
       FROM UNNEST(predicted_usertype_probs)
       WHERE label = 'Subscriber') AS subscriber_prob
    FROM ML.PREDICT(
      MODEL `oreilly.usertype_forest`,
      (
        SELECT
          tripduration,
          distance_m,
          start_station_name,
          start_hour,
          day_of_week
        FROM `oreilly.usertype_classification_features`
        -- filter rows to the scheduled run date; adjust the column name if different
        WHERE DATE(starttime) = run_date
          AND tripduration IS NOT NULL
          AND distance_m IS NOT NULL
          AND start_station_name IS NOT NULL
      )
    )
  )
);
-- end::scheduling_batch_queries[]

