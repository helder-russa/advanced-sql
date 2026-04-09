-- Kruskal-Wallis
-- tag::kruskal_wallis[]
WITH samples AS (
  SELECT
    STRUCT(
      species AS factor,
      CAST(body_mass_g AS FLOAT64) AS val
    ) AS sample_struct
  FROM `bigquery-public-data.ml_datasets.penguins`
  WHERE body_mass_g IS NOT NULL
)
SELECT
  bqutil.fn.kruskal_wallis(ARRAY_AGG(sample_struct)).H AS h_statistic,
  bqutil.fn.kruskal_wallis(ARRAY_AGG(sample_struct)).p AS p_value,
  bqutil.fn.kruskal_wallis(ARRAY_AGG(sample_struct)).dof AS degrees_of_freedom
FROM samples;
-- end::kruskal_wallis[]

