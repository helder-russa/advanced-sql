-- Multi-objective tuning for trip anomaly detection
-- tag::hparam_multi_objective[]
CREATE OR REPLACE MODEL `oreilly.trip_anomaly_multi_tuned`
OPTIONS(
  MODEL_TYPE = 'BOOSTED_TREE_CLASSIFIER',
  INPUT_LABEL_COLS = ['is_anomaly'],
  NUM_TRIALS = 15,
  MAX_PARALLEL_TRIALS = 3,
  HPARAM_TUNING_ALGORITHM = 'VIZIER_DEFAULT',
  HPARAM_TUNING_OBJECTIVES = ['PRECISION', 'RECALL'],
  MAX_TREE_DEPTH = HPARAM_CANDIDATES([3, 4, 5, 6]),
  MIN_TREE_CHILD_WEIGHT = HPARAM_RANGE(1, 5),
  REG_ALPHA = HPARAM_RANGE(0, 2)
) AS
SELECT * FROM `oreilly.trip_anomaly_features`
WHERE split_col = 'TRAIN';
-- end::hparam_multi_objective[]

