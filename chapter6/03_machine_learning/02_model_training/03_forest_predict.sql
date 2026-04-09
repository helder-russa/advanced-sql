-- Generate predictions on new data
-- tag::forest_predict[]
SELECT 
  predicted_usertype,
  predicted_usertype_probs,
  tripduration,
  start_hour,
  start_station_name
FROM ML.PREDICT(MODEL `oreilly.usertype_forest`,
  (
    SELECT * 
    FROM `oreilly.usertype_classification_features` 
    LIMIT 1000
   )
 )
ORDER BY predicted_usertype_probs[OFFSET(1)].prob DESC
LIMIT 20;
-- end::forest_predict[]

