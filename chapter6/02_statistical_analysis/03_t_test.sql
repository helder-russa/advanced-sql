-- t-test
-- tag::t_test[]
WITH daily_counts AS (
  SELECT
    DATE(date) AS crime_day,
    CAST(COUNT(*) AS FLOAT64) AS crimes,
    EXTRACT(MONTH FROM date) AS crime_month
  FROM `bigquery-public-data.chicago_crime.crime`
  WHERE
    DATE(date) BETWEEN '2022-01-01' AND '2022-01-31'
    OR DATE(date) BETWEEN '2022-07-01' AND '2022-07-31'
  GROUP BY crime_day, crime_month
),
samples AS (
  SELECT
    ARRAY_AGG(CASE WHEN crime_month = 1 THEN crimes END) AS jan_crimes,
    ARRAY_AGG(CASE WHEN crime_month = 7 THEN crimes END) AS jul_crimes
  FROM daily_counts
)
SELECT
  (SELECT AVG(x) FROM UNNEST(jan_crimes) x) AS jan_mean_daily_crimes,
  (SELECT AVG(x) FROM UNNEST(jul_crimes) x) AS jul_mean_daily_crimes,
  bqutil.fn.t_test(jan_crimes, jul_crimes).t_value AS t_statistic,
  bqutil.fn.t_test(jan_crimes, jul_crimes).dof AS degrees_of_freedom
FROM samples;
-- end::t_test[]

