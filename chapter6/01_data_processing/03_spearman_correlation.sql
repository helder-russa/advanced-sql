-- Spearman correlation
-- tag::spearman_correlation[]
WITH ranked AS (
  SELECT
    RANK() OVER (ORDER BY flipper_length_mm) AS rx,
    RANK() OVER (ORDER BY body_mass_g) AS ry
  FROM `bigquery-public-data.ml_datasets.penguins`
  WHERE flipper_length_mm IS NOT NULL
  AND body_mass_g IS NOT NULL
)
SELECT CORR(rx, ry) AS spearman_r
FROM ranked;
-- end::spearman_correlation[]

