-- Equal-width deciles from min/max
-- tag::equal_width_binning[]
-- Step A: compute min/max and the bin width
WITH stats AS (
  SELECT
    MIN(tripduration) AS vmin,
    MAX(tripduration) AS vmax
  FROM `bigquery-public-data.new_york_citibike.citibike_trips`
  WHERE tripduration IS NOT NULL
),
params AS (
  SELECT
    vmin,
    vmax,
    SAFE_DIVIDE(vmax - vmin, 10) AS width
  FROM stats
)

-- Step B: assign each row to a bin 1..10 using width
SELECT
  t.tripduration,
  CASE
    WHEN p.width IS NULL OR p.width = 0 THEN 1
    ELSE LEAST(
      10,
      1 + CAST(FLOOR( (t.tripduration - p.vmin) / p.width ) AS INT64)
    )
  END AS duration_decile_eqw
FROM `bigquery-public-data.new_york_citibike.citibike_trips` AS t
CROSS JOIN params AS p
WHERE t.tripduration IS NOT NULL
LIMIT 200;
-- end::equal_width_binning[]

