-- Dataset statistics for numerical columns - BigQuery
-- tag::dataset_stats[]
WITH unpivoted AS (
  SELECT *
  FROM `oreilly.weather`
  UNPIVOT (
    value FOR variable IN (
      Temp9am, Temp3pm, Humidity9am, Pressure9am, Pressure3pm, Rainfall
    )
  )
),
stats AS (
  SELECT
    variable,
    COUNT(value) AS n,
    COUNT(*) - COUNT(value) AS NumNulls,
    AVG(value) AS avg,
    STDDEV(value) AS stddev,
    VARIANCE(value) AS variance,
    MIN(value) AS min,
    MAX(value) AS max,
    APPROX_QUANTILES(value, 2)[OFFSET(1)] AS median
  FROM unpivoted
  GROUP BY variable
),
moment_calcs AS (
  SELECT
    u.variable,
    POWER(SUM(POWER(value - s.avg, 3)) / s.n, 1) / POWER(s.stddev, 3) AS skewness,
    POWER(SUM(POWER(value - s.avg, 4)) / s.n, 1) / POWER(s.variance, 2) AS kurtosis
  FROM unpivoted u
  JOIN stats s USING (variable)
  WHERE value IS NOT NULL
  GROUP BY u.variable, s.avg, s.stddev, s.variance, s.n
)
SELECT
  s.variable,
  s.n AS NumRecords,
  s.NumNulls,
  s.min AS Min,
  s.median AS Median,
  ROUND(s.avg, 2) AS Average,
  s.max AS Max,
  ROUND(s.stddev, 2) AS StdDev,
  ROUND(s.variance, 3) AS Variance,
  ROUND(m.skewness, 3) AS Skewness,
  ROUND(m.kurtosis, 3) AS Kurtosis
FROM stats s
JOIN moment_calcs m USING (variable)
ORDER BY variable;
-- end::dataset_stats[]

