-- Create sample weather dataset for Chapter 6 examples
-- This table is used to demonstrate dataset statistical description with UNPIVOT

CREATE OR REPLACE TABLE `oreilly.weather` AS
SELECT * FROM UNNEST([
  STRUCT(18.2 AS Temp9am, 24.5 AS Temp3pm, 72 AS Humidity9am, 1015.2 AS Pressure9am, 1012.8 AS Pressure3pm, 0.0 AS Rainfall),
  STRUCT(15.8, 22.1, 68, 1018.5, 1016.2, 2.4),
  STRUCT(21.3, 28.7, 55, 1012.1, 1008.9, 0.0),
  STRUCT(12.5, 18.3, 85, 1020.8, 1018.5, 8.6),
  STRUCT(19.7, 26.2, 62, 1014.3, 1011.7, 0.2),
  STRUCT(16.4, 23.8, 78, 1016.9, 1014.1, 5.1),
  STRUCT(22.8, 31.5, 48, 1009.5, 1005.8, 0.0),
  STRUCT(14.1, 19.6, 88, 1022.1, 1019.8, 12.3),
  STRUCT(17.9, 25.1, 65, 1017.2, 1014.6, 0.8),
  STRUCT(20.5, 27.9, 58, 1013.8, 1010.4, 0.0),
  STRUCT(NULL, 21.4, 71, 1019.1, NULL, 3.2),
  STRUCT(13.2, NULL, 82, NULL, 1017.3, 6.5)
]);

