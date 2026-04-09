-- Confidence intervals for normal approximation, 95% confidence level
-- tag::confidence_intervals[]
SELECT
  year AS grp,
  AVG(mother_age) AS mean_age,
  STDDEV_SAMP(mother_age) AS s,
  COUNT(*) AS n,
  -- Replace [z] with desired z/t critical value, e.g., 1.96 for ~95% CI
  AVG(mother_age) - 1.96 * STDDEV_SAMP(mother_age) / SQRT(COUNT(*)) AS ci_lower,
  AVG(mother_age) + 1.96 * STDDEV_SAMP(mother_age) / SQRT(COUNT(*)) AS ci_upper
FROM `bigquery-public-data.samples.natality`
WHERE mother_age IS NOT NULL
GROUP BY year
ORDER BY year;
-- end::confidence_intervals[]

