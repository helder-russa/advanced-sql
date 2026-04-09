-- One-Hot encoding
-- tag::one_hot[]
SELECT
  usertype,
  IF(usertype = 'Customer',   1, 0) AS usertype_customer,
  IF(usertype = 'Subscriber', 1, 0) AS usertype_subscriber
FROM `bigquery-public-data.new_york_citibike.citibike_trips`
WHERE usertype IS NOT NULL
LIMIT 100;
-- end::one_hot[]

