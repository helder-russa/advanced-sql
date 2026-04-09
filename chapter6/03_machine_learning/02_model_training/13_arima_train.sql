-- Train ARIMA_PLUS forecasting model
-- tag::arima_train[]
CREATE OR REPLACE MODEL `oreilly.trip_volume_forecast`
OPTIONS(
  MODEL_TYPE = 'ARIMA_PLUS',
  TIME_SERIES_TIMESTAMP_COL = 'trip_date',
  TIME_SERIES_DATA_COL     = 'daily_trips',
  HOLIDAY_REGION           = 'US',
  AUTO_ARIMA               = TRUE,
  DATA_FREQUENCY           = 'DAILY'
) AS
SELECT trip_date, daily_trips
FROM `oreilly.daily_trip_volume`;
-- end::arima_train[]

