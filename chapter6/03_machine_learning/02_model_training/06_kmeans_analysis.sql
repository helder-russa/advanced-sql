-- Analyze cluster characteristics
-- tag::kmeans_analysis[]
WITH predictions AS (
  SELECT 
    s.start_station_name,
    s.total_trips,
    s.avg_duration,
    p.CENTROID_ID as cluster_id
  FROM `oreilly.station_clustering_features` s
  JOIN ML.PREDICT(MODEL `oreilly.station_clusters`, 
                  TABLE `oreilly.station_clustering_features`) p
    ON s.start_station_name = p.start_station_name
)
SELECT 
  cluster_id,
  COUNT(*) as stations_in_cluster,
  ROUND(AVG(total_trips)) as avg_trips_per_station,
  ROUND(AVG(avg_duration)) as avg_trip_duration
FROM predictions
GROUP BY cluster_id
ORDER BY cluster_id;
-- end::kmeans_analysis[]

