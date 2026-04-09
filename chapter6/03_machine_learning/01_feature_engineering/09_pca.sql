-- PCA - Principal Component Analysis (Complete Workflow)
-- tag::pca[]

-- Step 1: Create feature table
-- tag::pca_features[]
CREATE OR REPLACE TABLE `oreilly.citibike_features` AS
SELECT
  tripduration,
  EXTRACT(HOUR FROM starttime) AS start_hour,
  ST_DISTANCE(
    ST_GEOGPOINT(start_station_longitude, start_station_latitude),
    ST_GEOGPOINT(end_station_longitude, end_station_latitude)
  ) AS distance_m
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE start_station_latitude  IS NOT NULL 
  AND end_station_latitude  IS NOT NULL
  AND start_station_longitude IS NOT NULL 
  AND end_station_longitude IS NOT NULL;
-- end::pca_features[]

-- Step 2: Train PCA with a variance threshold
-- tag::pca_train[]
CREATE OR REPLACE MODEL `oreilly.pca_citibike`
OPTIONS(
  MODEL_TYPE = 'PCA',
  PCA_EXPLAINED_VARIANCE_RATIO = 0.95
  -- NUM_PRINCIPAL_COMPONENTS = 2
) AS
SELECT tripduration, start_hour, distance_m
FROM `oreilly.citibike_features`;
-- end::pca_train[]

-- Step 3: Score rows with principal components
-- tag::pca_project[]
SELECT *
FROM ML.PREDICT(
  MODEL `oreilly.pca_citibike`,
  TABLE `oreilly.citibike_features`
)
LIMIT 100;
-- end::pca_project[]

-- Step 4: View component loadings and variance info
-- tag::pca_eigenvalues[]
SELECT *
FROM ML.ML.PRINCIPAL_COMPONENT_INFO(MODEL `oreilly.pca_citibike`);
-- end::pca_eigenvalues[]
-- end::pca[]

