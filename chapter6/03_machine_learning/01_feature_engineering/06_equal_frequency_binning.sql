-- Equal-frequency deciles with PERCENTILE_CONT (exact cut points)
-- tag::equal_frequency_binning[]
-- Step A: exact cut points (deciles)
WITH bounds AS (
  SELECT
    PERCENTILE_CONT(tripduration, 0.1) OVER() AS p10,
    PERCENTILE_CONT(tripduration, 0.2) OVER() AS p20,
    PERCENTILE_CONT(tripduration, 0.3) OVER() AS p30,
    PERCENTILE_CONT(tripduration, 0.4) OVER() AS p40,
    PERCENTILE_CONT(tripduration, 0.5) OVER() AS p50,
    PERCENTILE_CONT(tripduration, 0.6) OVER() AS p60,
    PERCENTILE_CONT(tripduration, 0.7) OVER() AS p70,
    PERCENTILE_CONT(tripduration, 0.8) OVER() AS p80,
    PERCENTILE_CONT(tripduration, 0.9) OVER() AS p90
  FROM `bigquery-public-data.new_york_citibike.citibike_trips`
  WHERE tripduration IS NOT NULL
  LIMIT 1
)

-- Step B: assign each row to a bin 1..10 via CASE
SELECT
  t.tripduration,
  CASE
    WHEN t.tripduration <  b.p10 THEN 1
    WHEN t.tripduration <  b.p20 THEN 2
    WHEN t.tripduration <  b.p30 THEN 3
    WHEN t.tripduration <  b.p40 THEN 4
    WHEN t.tripduration <  b.p50 THEN 5
    WHEN t.tripduration <  b.p60 THEN 6
    WHEN t.tripduration <  b.p70 THEN 7
    WHEN t.tripduration <  b.p80 THEN 8
    WHEN t.tripduration <  b.p90 THEN 9
    ELSE 10
  END AS duration_decile_eqf
FROM `bigquery-public-data.new_york_citibike.citibike_trips` AS t
CROSS JOIN bounds AS b
WHERE t.tripduration IS NOT NULL
LIMIT 200;
-- end::equal_frequency_binning[]

