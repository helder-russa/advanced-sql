-- Generate future predictions
-- tag::generate_forecast[]
SELECT
  forecast_timestamp,
  forecast_value,
  standard_error,
  confidence_level,
  prediction_interval_lower_bound,
  prediction_interval_upper_bound
FROM ML.FORECAST(
  MODEL `oreilly.trip_volume_forecast`,
  STRUCT(30 AS horizon, 0.95 AS confidence_level)
)
ORDER BY forecast_timestamp;
-- end::generate_forecast[]

