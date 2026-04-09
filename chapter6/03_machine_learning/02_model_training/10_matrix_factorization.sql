-- Train matrix factorization model
-- tag::matrix_factorization[]
CREATE OR REPLACE MODEL `oreilly.station_recommender`
OPTIONS(
  MODEL_TYPE = 'MATRIX_FACTORIZATION',
  USER_COL = 'user_id',
  ITEM_COL = 'station_id', 
  RATING_COL = 'rating',
  NUM_FACTORS = 50,
  L2_REG = 0.1,
  MAX_ITERATIONS = 30
) AS
SELECT 
  user_id,
  station_id,
  rating
FROM `oreilly.user_station_interactions`;
-- end::matrix_factorization[]

