-- Kolmogorov–Smirnov
-- tag::ks_test[]
WITH base AS (
  SELECT mother_age AS x
  FROM `bigquery-public-data.samples.natality`
  WHERE mother_age IS NOT NULL
  LIMIT 10000  -- Optional: limit for performance
),
fit AS (
  SELECT AVG(x) AS mu, STDDEV_SAMP(x) AS sigma, COUNT(*) AS n FROM base
),
ecdf AS (
  SELECT
    x,
    ROW_NUMBER() OVER (ORDER BY x) * 1.0 / COUNT(*) OVER () AS F_emp
  FROM base
),
theory AS (
  SELECT
    e.x,
    e.F_emp,
    bqutil.fn.normal_cdf(e.x, f.mu, f.sigma) AS F_norm
  FROM ecdf e CROSS JOIN fit f
)
SELECT
  MAX(ABS(F_emp - F_norm)) AS D_stat
FROM theory;
-- end::ks_test[]

