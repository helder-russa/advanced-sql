-- Mann–Whitney U
-- tag::mann_whitney[]
WITH samples AS (
  SELECT
    ARRAY_AGG(CASE WHEN sex = 'MALE' THEN body_mass_g END IGNORE NULLS) AS male_mass,
    ARRAY_AGG(CASE WHEN sex = 'FEMALE' THEN body_mass_g END IGNORE NULLS) AS female_mass
  FROM `bigquery-public-data.ml_datasets.penguins`
  WHERE
    sex IN ('MALE', 'FEMALE')
    AND body_mass_g IS NOT NULL
)
SELECT
  bqutil.fn.mannwhitneyu(male_mass, female_mass, 'two-sided').U AS u_statistic,
  bqutil.fn.mannwhitneyu(male_mass, female_mass, 'two-sided').p AS p_value
FROM samples;
-- end::mann_whitney[]

