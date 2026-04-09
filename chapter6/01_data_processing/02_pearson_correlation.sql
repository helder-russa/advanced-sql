-- Pearson correlation
-- tag::pearson_correlation[]
SELECT
  CORR(flipper_length_mm, body_mass_g) AS r
FROM `bigquery-public-data.ml_datasets.penguins`
WHERE flipper_length_mm IS NOT NULL
  AND body_mass_g IS NOT NULL;
-- end::pearson_correlation[]

