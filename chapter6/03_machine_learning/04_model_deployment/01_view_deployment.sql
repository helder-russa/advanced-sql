-- Deploy model as a view for predictions
-- tag::view_deployment[]
CREATE OR REPLACE VIEW `oreilly.usertype_scores_latest` AS
SELECT
  'Subscriber' AS label,
  subscriber_prob AS predicted_probability,
  CASE
    WHEN subscriber_prob >= 0.40 THEN 'Subscriber'
    ELSE 'Customer'
  END AS business_decision,
  tripduration,
  distance_m,
  start_station_name,
  start_hour
FROM (
  SELECT
    tripduration,
    distance_m,
    start_station_name,
    start_hour,
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
      WHERE tripduration IS NOT NULL
        AND distance_m IS NOT NULL
        AND start_station_name IS NOT NULL
    )
  )
);
-- end::view_deployment[]

