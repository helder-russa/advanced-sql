CREATE OR REPLACE EXTERNAL TABLE `advance-sql-de-demo.bronze.orders`
WITH CONNECTION `advance-sql-de-demo.europe-west1.adv-sql-de-bigquery-biglake-conn`
OPTIONS (
  format = 'ICEBERG',
  uris = [
    'gs://advance-sql-de-bucket/iceberg/warehouse/bronze/orders/metadata/v2.metadata.json'
  ]
);