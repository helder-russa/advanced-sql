# Chapter 05 - Consumers – Setup Guide

This folder contains the **consumer side** of the Chapter 5 pipeline.  
It ingests data from **streaming (Pub/Sub)** and **batch APIs**, lands it in **GCS (Landing Zone)**, and materializes **Iceberg tables (Bronze layer)** using Spark.

> ⚠️ **Important**
>
> - Producers **must be running first**.
> - This chapter intentionally demonstrates **both streaming and batch ingestion**.
> - Spark is used **explicitly** as the ingestion and structuring engine.
> - Resource constraints (Dataproc Serverless quotas) are real and documented.

⚠️ Ensure that all CLI executions which reference local files are run from the root directory `ÀDVANCED_SQL`

---

## Architecture at a Glance
```bash
Producers (API + Pub/Sub)
        |
        v
+--------------------+
|   Landing Zone     |  (GCS – raw JSON / NDJSON)
|  - streaming/      |
|  - batch/          |
+--------------------+
        |
        v
+--------------------+
|  Bronze Layer      |  (Apache Iceberg on GCS)
|  - orders          |
|  - customers       |
|  - products        |
+--------------------+
        |
        v
(BigQuery / dbt – later chapters, not included in this readme)
```
## 1. Streaming ingestion (Pub/Sub → GCS Landing Zone)

### 1.1 Environment variables
Configure the environment variables that would be used for downstream executions. Arrange them accordingly to your project configurations.

```bash
export PROJECT_ID="advance-sql-de-demo"
export REGION="europe-west1"
export BUCKET="advance-sql-de-bucket"
```

### 1.2 Create the definitive subscription
We will start by consuming from pub/sub, so in your terminal let's start by creating the definitive subscription list:
```bash
gcloud config set project "$PROJECT_ID"

gcloud pubsub subscriptions create orders-to-gcs-sub \
  --topic=orders
```

👉 This replaces the temporary debug subscription used in the producers (`orders-debug-sub`).

### 1.3 Service Account (streaming ingestion)
```bash
gcloud iam service-accounts create ch05-streaming-ingestion-sa \
  --display-name="Streaming Ingestion SA"
```

Grant minimum permissions:
```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:ch05-streaming-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/pubsub.subscriber"

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:ch05-streaming-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator"
```
### 1.4 Deploy the Cloud Run Job (streaming)
This job continuously listens to **Pub/Sub** for new events and stores them as raw JSON files in the **Landing Zone** inside your `$BUCKET`.

The Landing Zone acts as the system’s entry point: data is captured exactly as produced, without transformation, and made available for downstream processing.

```bash
JOB_NAME="ch05-streaming-ingestion-job"

gcloud run jobs deploy "$JOB_NAME" \
  --region="$REGION" \
  --source="chapter_05/consumers/streaming_ingestion" \
  --service-account="ch05-streaming-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,SUBSCRIPTION_ID=orders-to-gcs-sub,BUCKET=$BUCKET,GCS_PREFIX=landing_zone/streaming/orders,MAX_MESSAGES=500,MAX_LOOPS=20,PULL_TIMEOUT=10"
```

Execute the job:

```bash
gcloud run jobs execute "$JOB_NAME" --region="$REGION"
```

Data should land in your google cloud storage defined bucket:
`gs://$BUCKET/landing_zone/streaming/orders/`

If this is not the case, please check the logs available in **Cloud Run → Jobs**.

## 2. Streaming → Bronze (Spark Structured Streaming + Iceberg)
The following job **reads files from the Landing Zone** and writes to an **Iceberg Bronze table**.

```bash 
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/streaming/01_lz_orders_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dataproc.driver.disk.size=250g,\
spark.dataproc.executor.disk.size=250g,\
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.local.type=hadoop,\
spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --GCS_BUCKET="$BUCKET" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/streaming/orders/" \
  --CHECKPOINT_PATH="gs://$BUCKET/_checkpoints/ch05/orders_to_iceberg/" \
  --QUARANTINE_PATH="gs://$BUCKET/quarantine/live_orders/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="live_orders" \
  --MAX_FILES_PER_TRIGGER="50" \
  --TRIGGER_SECONDS="15" \
  --SPARK_LOG_LEVEL="WARN"
```

## 3. Batch ingestion (API → GCS Landing Zone)
Batch ingestion is implemented using **Cloud Run Jobs**, with **one job per entity**.

Unlike streaming ingestion, where data is pulled from Pub/Sub. batch ingestion retrieves data directly from the API exposed by the **Producers** service.

### 3.1 Service Account (batch ingestion)
```bash
gcloud iam service-accounts create ch05-batch-ingestion-sa \
  --display-name="Batch Ingestion SA"

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectCreator" 

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user" 

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor" 

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquerydatatransfer.serviceAgent" 
```

### 3.2 Deploy batch jobs
These jobs, one per entity, are responsible to pull the data from API and store it in the `landing_zone/batch` within your `$BUCKET`.

Each batch job is responsible for:
- Calling the Producers API for a specific entity
- Enriching records with ingestion metadata
- Writing immutable snapshot files to `landing_zone/batch` inside your `$BUCKET`

```bash
export API_BASE_URL="https://ch05-producers-api-xxxxx.europe-west1.run.app/"

gcloud run jobs deploy ch05-batch-ingestion-customers \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --source="chapter_05/consumers/batch_ingestion" \
  --service-account="ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --command="python" \
  --args="main.py" \
  --set-env-vars="BUCKET=$BUCKET,GCS_PREFIX=landing_zone/batch,API_BASE_URL=$API_BASE_URL,HTTP_TIMEOUT=30,ENTITY=customers"


gcloud run jobs deploy ch05-batch-ingestion-products \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --source="chapter_05/consumers/batch_ingestion" \
  --service-account="ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --command="python" \
  --args="main.py" \
  --set-env-vars="BUCKET=$BUCKET,GCS_PREFIX=landing_zone/batch,API_BASE_URL=$API_BASE_URL,HTTP_TIMEOUT=30,ENTITY=products"

gcloud run jobs deploy ch05-batch-ingestion-orders \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --source="chapter_05/consumers/batch_ingestion" \
  --service-account="ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --command="python" \
  --args="main.py" \
  --set-env-vars="BUCKET=$BUCKET,GCS_PREFIX=landing_zone/batch,API_BASE_URL=$API_BASE_URL,HTTP_TIMEOUT=30,ENTITY=orders"

```
Execute as needed, per entity:
```bash
gcloud run jobs execute ch05-batch-ingestion-customers \
  --project="${PROJECT_ID}" \
  --region="${REGION}"

  gcloud run jobs execute ch05-batch-ingestion-products \
  --project="${PROJECT_ID}" \
  --region="${REGION}"

  gcloud run jobs execute ch05-batch-ingestion-orders \
  --project="${PROJECT_ID}" \
  --region="${REGION}" 
```

Data should land in your google cloud storage defined bucket:
`gs://$BUCKET/landing_zone/batch/{entity}/snapshot_dt=YYYY-MM-DD/`

## 4. Batch → Bronze (Spark + Iceberg)
Run once per entity, as well:

```bash
:: customers execution
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/batch/01_lz_batch_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dataproc.driver.disk.size=250g,\
spark.dataproc.executor.disk.size=250g,\
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.local.type=hadoop,\
spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --ENTITY="customers" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/batch/customers/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="customers" \
  --RESET_TABLE="false" \
  --SPARK_LOG_LEVEL="WARN"

:: products execution
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/batch/01_lz_batch_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dataproc.driver.disk.size=250g,\
spark.dataproc.executor.disk.size=250g,\
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.local.type=hadoop,\
spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --ENTITY="products" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/batch/products/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="products" \
  --RESET_TABLE="false" \
  --SPARK_LOG_LEVEL="WARN"

:: orders execution
gcloud dataproc batches submit pyspark \
  "chapter_05/consumers/spark/jobs/batch/01_lz_batch_to_bronze_layer.py" \
  --project="$PROJECT_ID" \
  --region="$REGION" \
  --deps-bucket="gs://$BUCKET" \
  --ttl="10m" \
  --properties="spark.executor.instances=2,\
spark.driver.cores=4,\
spark.executor.cores=4,\
spark.dataproc.driver.disk.size=250g,\
spark.dataproc.executor.disk.size=250g,\
spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.13:1.10.1,\
spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,\
spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog,\
spark.sql.catalog.local.type=hadoop,\
spark.sql.catalog.local.warehouse=gs://$BUCKET/iceberg/warehouse" \
  -- \
  --ENTITY="orders" \
  --INPUT_PATH="gs://$BUCKET/landing_zone/batch/orders/" \
  --CATALOG_NAME="local" \
  --DB_NAME="bronze" \
  --TABLE_NAME="orders" \
  --RESET_TABLE="true" \
  --SPARK_LOG_LEVEL="WARN"
```
## 5. Automated refresh of BigQuery tables to last vN.metadata.json

### 5.1 Deploy batch jobs
You can do it by hand, just executing per entity the `CREATE OR REPLACE EXTERNAL TABLE advance-sql-de-demo.bronze.ENTITY` code within `chapter_05/sql/ddl` folder, updating the respective URI for the new metadata file. The following job executes a script that automates it, leveraging cloud run.

```bash
BQ_CONNECTION="projects/$PROJECT_ID/locations/$REGION/connections/<REPLACE BY YOUR BQ BIGLAKE CONNECTION>"

BQ_DATASET="bronze"

gcloud run jobs deploy ch05-refresh-bq-iceberg-tables \
  --region="$REGION" \
  --source="chapter_05/consumers/helper/refresh_bq_iceberg_tables" \
  --service-account="ch05-batch-ingestion-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,BQ_DATASET=$BQ_DATASET,BQ_CONNECTION=$BQ_CONNECTION,WAREHOUSE_PREFIX=iceberg/warehouse,ICEBERG_DB=bronze"
```

Execute the job:

```bash
gcloud run jobs execute ch05-refresh-bq-iceberg-tables --region="$REGION"
```

## Troubleshooting

### Pub/Sub job runs but no data appears in GCS

To tackle this issue, check some of the following scenarios:

- Producers are actively publishing events.
- The subscription name is correct.
- The service account has:
    - `roles/pubsub.subscriber`
    - `roles/storage.objectCreator`

### Dataproc batch fails with quota errors

Common messages:
```bash
Insufficient 'CPUS_ALL_REGIONS'
Insufficient 'DISKS_TOTAL_GB'
```

Actions:

- Ensure **no other Dataproc batches are running**.
    - This is a frequent issue with personal GCP accounts, where quotas are intentionally kept at minimal levels for user protection.
    - Check which Dataproc batches are running by executing: `gcloud dataproc batches list --region=$REGION`
    - If one or more batches are in a `RUNNING` or `PENDING` state and are no longer needed, cancel them using: `gcloud dataproc batches cancel <BATCH_ID> --region=$REGION`

Reduce (if not in the minimum):
- `spark.executor.instances`
- job `--ttl`

### `PATH_NOT_FOUND` when reading from GCS
Spark expects the **directory to exist**.

Ensure:
- Downstream processes ran successfully and, at least, one file has been written to the Landing Zone.
- The path matches exactly:
    - `gs://$BUCKET/landing_zone/<streaming|batch>/<entity>/`

### Iceberg write errors (schema mismatch)
If you see:
```bash
Cannot find data for output column
```

It means:
- The Iceberg table schema does not match the DataFrame.
- The table was created earlier with a different schema.

Fix:
- Drop the table (for demos), like:
```bash
DROP TABLE bronze.orders;
```
- Within the Batch-to-Bronze data movement process, the Dataproc execution includes a field named `RESET_TABLE`. When set to `true`, this field drops the local column table associated with the specified entity.