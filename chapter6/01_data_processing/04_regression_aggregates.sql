-- Regression‑lite from aggregates
-- tag::regression_aggregates[]
WITH s AS (
  SELECT
    AVG(weight_pounds)                         AS avg_y,
    AVG(gestation_weeks)                       AS avg_x,
    COVAR_POP(weight_pounds, gestation_weeks)  AS cov,
    VAR_POP(gestation_weeks)                   AS varx,
    CORR(weight_pounds, gestation_weeks)       AS r
  FROM `bigquery-public-data.samples.natality`
  WHERE weight_pounds IS NOT NULL
    AND gestation_weeks IS NOT NULL
    -- Trim implausible values to limit the effect of extremes
    AND weight_pounds BETWEEN 3 AND 14       
    AND gestation_weeks BETWEEN 30 AND 45     
)
SELECT
  cov / varx                           AS slope,
  avg_y - (cov / varx) * avg_x         AS intercept,
  POW(r, 2)                            AS r2
FROM s;
-- end::regression_aggregates[]

