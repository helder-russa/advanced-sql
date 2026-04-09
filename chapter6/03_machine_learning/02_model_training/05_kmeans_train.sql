-- Train k-means clustering model
-- tag::kmeans_train[]
CREATE OR REPLACE MODEL `oreilly.station_clusters`
OPTIONS(
  MODEL_TYPE = 'KMEANS',
  NUM_CLUSTERS = 5,
  STANDARDIZE_FEATURES = TRUE,
  MAX_ITERATIONS = 50,
  KMEANS_INIT_METHOD = 'KMEANS++',
  DISTANCE_TYPE = 'EUCLIDEAN'
) AS
SELECT 
  total_trips,
  avg_duration,
  stddev_duration,
  active_days,
  avg_start_hour,
  user_types
FROM `oreilly.station_clustering_features`;
-- end::kmeans_train[]

