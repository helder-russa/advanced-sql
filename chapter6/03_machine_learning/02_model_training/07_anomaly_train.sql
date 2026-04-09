-- Train autoencoder for anomaly detection
-- tag::anomaly_train[]
CREATE OR REPLACE MODEL `oreilly.trip_anomaly_detector`
OPTIONS(
  MODEL_TYPE = 'AUTOENCODER',
  ACTIVATION_FN = 'RELU',
  BATCH_SIZE = 1000,
  DROPOUT = 0.2,
  HIDDEN_UNITS = [10, 4, 2, 4, 10],  -- Encoding-decoding structure
  MAX_ITERATIONS = 100,
  LEARN_RATE = 0.001,
  OPTIMIZER = 'ADAM'
) AS
SELECT 
  tripduration,
  start_hour,
  day_of_week,
  distance_m
FROM `oreilly.usertype_classification_features`;
-- end::anomaly_train[]

