-- Train random forest classifier
-- tag::forest_train[]
CREATE OR REPLACE MODEL `oreilly.usertype_forest`
OPTIONS(
  MODEL_TYPE = 'RANDOM_FOREST_CLASSIFIER',
  INPUT_LABEL_COLS = ['usertype'],
  NUM_PARALLEL_TREE = 100,
  MAX_TREE_DEPTH = 10,
  SUBSAMPLE = 0.8,
  MIN_SPLIT_LOSS = 0.1
) AS
SELECT 
  usertype,
  tripduration,
  start_hour,
  day_of_week, 
  start_station_name,
  distance_m,
  starttime
FROM `oreilly.usertype_classification_features`;
-- end::forest_train[]

