# Chapter 5 — Building Data Engineering Solutions with SQL (End-to-End Pipeline)

This chapter implements an end-to-end, SQL-friendly data pipeline on Google Cloud, designed to support both:
- **Streaming ingestion** (near real-time orders)
- **Batch ingestion** (full snapshots of customers, products, and orders)

The goal is to demonstrate a modern lakehouse-style flow where:
- **raw JSON lands in GCS** (Landing Zone)
- **Spark writes structured tables to Apache Iceberg** (Bronze)
- **dbt builds Silver/Gold layers and a semantic layer using SQL** (BigQuery as the query engine)

---

## Repository structure (Chapter 5)

High-level layout:

- `producers/`  
  Source systems (APIs) that generate `customers`, `products`, and `orders`, plus a streaming order event producer to Pub/Sub.

- `consumers/`  
  Ingestion + Spark jobs:
  - Cloud Run Jobs that write data into the **Landing Zone**
  - Dataproc Serverless Spark jobs that write to **Iceberg (Bronze)**

- `transformations/dbt/ch05_dbt_transformations/`  
  dbt project that builds:
  - Silver models (cleaned, conformed, current/latest snapshot)
  - Gold models (analytics-ready and semantic layer)
  - tests + documentation

---

## Data journey (mental model)
```bash
Producers
↓
Landing Zone (GCS) — raw JSON files
↓
Bronze (Iceberg on GCS) — structured tables
↓
BigQuery External Tables (read Iceberg metadata)
↓
Silver / Gold (dbt in BigQuery) — SQL transformations + semantic layer
```

Notes:
- `landing_zone/` is the raw drop area (JSON).
- `bronze.*` tables are Iceberg tables stored on GCS and queried from BigQuery.
- dbt materializes Silver/Gold datasets in BigQuery.

---

## Prerequisites

This README assumes you already followed the setup instructions in:
- `chapter_05/producers/README.md`
- `chapter_05/consumers/README.md`
- `chapter_05/transformations/README.md`

If anything fails, start by re-checking those READMEs first.

---

## Execution order (end-to-end)

### 1. Set environment variables (used by all commands)
```bash
export PROJECT_ID="advance-sql-de-demo"
export REGION="europe-west1"
export BUCKET="advance-sql-de-bucket"
```

### 2. Producers (make sure the source systems are running)
Producers must be deployed and reachable (Cloud Run), because:
- Streaming ingestion reads from Pub/Sub events produced by the Orders producer
- Batch ingestion pulls snapshots from the Producers API

Ultimately, if your `https://ch05-producers-api-xxxxx.a.run.app/docs` is running, and you can generate data, you can move to the next step, otherwise follow `chapter_05/producers/README.md` for producers setup guide.

### 3. Streaming ingestion (Pub/Sub → Landing Zone → Iceberg Bronze)

#### 3.1 Run Cloud Run Job: Pub/Sub → Landing Zone
This job writes streaming orders as raw JSON into:
- `gs://$BUCKET/landing_zone/streaming/orders/`

```bash
gcloud run jobs execute ch05-streaming-ingestion-job \
  --region="$REGION"
```

#### 3.2 Run Spark job: Landing Zone → Iceberg Bronze (live orders)
This job reads new JSON files from the Landing Zone using Structured Streaming
and writes Iceberg tables in Bronze.

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
👉 Expected result:
- Data inside: `gs://$BUCKET/iceberg/warehouse/bronze/<entity>/data/ingest_data=.../` in a `parquet`file format.
- Then after BigQuery Iceberg table creation (step 5 helps with that) the data can also be seen at `bronze.live_orders`

### 4. Batch ingestion (API → Landing Zone → Iceberg Bronze)
#### 4.1 Run Cloud Run batch ingestion job (one execution per entity)
This job writes full snapshot JSON into:
- `gs://$BUCKET/landing_zone/batch/<entity>/snapshot_dt=.../snapshot_ts=.../`

Execute each entity job:
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

#### 4.2 Run Spark job: Landing Zone batch → Iceberg Bronze (per entity)
Run once per entity (customers, products, orders).

⚠️ REMINDER the dataproc quota limits, ensure the complete termination of the job before running the next.


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

👉 Expected result:
- Data inside: `gs://$BUCKET/iceberg/warehouse/bronze/<entity>/data/ingest_data=.../` in a `parquet`file format.
- Then after BigQuery Iceberg table creation (step 5 helps with that) the data can also be seen at `bronze.<entity>`

### 5. Refresh BigQuery external tables (Iceberg metadata moves)
Iceberg metadata evolves (v1 → v2 → v3…), and BigQuery external tables may need a refresh.

```bash
gcloud run jobs execute ch05-refresh-bq-iceberg-tables --region="$REGION"
```

Expected result:
- BigQuery sees the latest snapshot/metadata without manual table recreation

👉 This can also be done manual, for that please refer to each DDL file inside `chapter_05/sql/ddl`
👉 Make sure the database `bronze`database schema is created. The reamining `silver` and `gold` dbt handles it automatically.

### 6. Transformations with dbt (Bronze → Silver → Gold)
Run dbt after Bronze tables are populated and visible in BigQuery.
```bash
gcloud run jobs execute ch05-dbt-job \
  --project="${PROJECT_ID}" \
  --region="${REGION}"
```

Expected result:
- BigQuery datasets:
  - `silver` (clean + current snapshot)
  - `gold` (analytics layer + semantic models)
- dbt tests and docs available
  - docs inside the bucket created specifically for the purpose: `gs://$DOCS_BUCKET/dbt_docs/index.html` public URL.
---

## What “done” looks like

At the end of this chapter, you should have:

### Landing Zone (GCS)
- `landing_zone/streaming/orders/...`
- `landing_zone/batch/customers/...`
- `landing_zone/batch/products/...`
- `landing_zone/batch/orders/...`

### Iceberg Bronze (GCS + BigQuery external tables)
- `bronze.live_orders`
- `bronze.customers`
- `bronze.products`
- `bronze.orders`

### dbt layers (BigQuery managed tables)
- `silver.*`
- `gold.*`