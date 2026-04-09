-- Generate station recommendations
-- tag::generate_recommendations[]
SELECT 
  user_id,
  station_id,
  predicted_rating
FROM ML.RECOMMEND(
  MODEL `oreilly.station_recommender`,
  (SELECT 'user_1234' as user_id)
 )  -- Recommend for specific user
WHERE predicted_rating IS NOT NULL
ORDER BY predicted_rating DESC
LIMIT 10;
-- end::generate_recommendations[]

