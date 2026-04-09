-- Train with automated tuning
-- tag::hparam_xgb_train[]
CREATE OR REPLACE MODEL `oreilly.usertype_xgb_tuned`
OPTIONS(
  MODEL_TYPE = 'BOOSTED_TREE_CLASSIFIER',
  INPUT_LABEL_COLS = ['usertype'],
  NUM_TRIALS = 20,
  MAX_PARALLEL_TRIALS = 2,
  HPARAM_TUNING_ALGORITHM = 'VIZIER_DEFAULT',
  HPARAM_TUNING_OBJECTIVES = ['ROC_AUC'],
  LEARN_RATE = HPARAM_RANGE(0.01, 0.3),
  MAX_TREE_DEPTH = HPARAM_CANDIDATES([4, 6, 8, 10]),
  SUBSAMPLE = HPARAM_RANGE(0.6, 1.0)
) AS
SELECT 
  usertype, 
  tripduration, 
  start_hour, 
  day_of_week, 
  start_station_name, 
  distance_m
FROM `oreilly.usertype_classification_features`;
-- end::hparam_xgb_train[]

