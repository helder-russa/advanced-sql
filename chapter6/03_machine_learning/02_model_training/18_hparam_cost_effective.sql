-- Cost-effective two-phase tuning approach
-- tag::hparam_cost_effective[]
CREATE OR REPLACE MODEL `oreilly.station_clusters_explore`
OPTIONS(
  MODEL_TYPE = 'KMEANS',
  NUM_CLUSTERS = HPARAM_CANDIDATES([3, 5, 7, 10]),
  NUM_TRIALS = 8,
  HPARAM_TUNING_OBJECTIVES = ['DAVIES_BOULDIN_INDEX']
) AS
SELECT * FROM `oreilly.station_clustering_features`
WHERE RAND() < 0.2; 
-- end::hparam_cost_effective[]

