-- Frequency encoding
-- tag::frequency_encoding[]
WITH freq AS (
  SELECT category, COUNT(*) AS category_freq
  FROM `bigquery-public-data.thelook_ecommerce.products`
  GROUP BY category
)
SELECT
  id,
  p.category,
  f.category_freq
FROM `bigquery-public-data.thelook_ecommerce.products` p
JOIN freq f USING (category)
LIMIT 100;
-- end::frequency_encoding[]

