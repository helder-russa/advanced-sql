# Chapter 05 - Orchestration – Setup Guide

This folder contains the **Cloud Workflows** that coordinate ingestion pipelines for Chapter 05. The orchestration is separated into **batch** and **streaming** flows.

---

## Orchestration at a Glance
```bash
  +-----------------------------+
  |     ch05-wf-ingest-*        |     (Cloud Workflows)
  |  - customers, products,     |
  |    orders, live_orders      |
  +--------------+--------------+
                |
                v
    +-----------------------+
    |    Cloud Run Jobs     |     (Ingest batch and streaming)
    +-----------------------+
                |
                v
    +-----------------------+
    |     Landing Zone      |     (GCS raw JSON)
    | /batch/ + /streaming/ |
    +-----------------------+
                |
    +-----------------------+
    |    Dataproc batches   |     (Batch Spark & Spark Structured Streaming)
    +-----------------------+            
                v
    +------------------------+
    |     Bronze Layer       |     (Iceberg on GCS)
    |   bronze.orders        |
    |   bronze.customers     |
    |   bronze.products      |
    |   bronze.live_orders   |
    +------------------------+
                |
                v
(BigQuery refresh tables + dbt later steps)
```
## 1. Environment Variables

Before deploying any workflow, export your environment configuration:

```bash
export PROJECT_ID="advance-sql-de-demo"
export REGION="europe-west1"
export BUCKET="advance-sql-de-bucket"
export SERVICE_ACCOUNT="<YOUR_SERVICE_ACCOUNT_EMAIL_WITH_REQUIRED_PERMISSIONS>"
```

## 2 Deploy workflows
Each workflow is scoped to a specific ingestion task for improved control. The `ch05-wf-full-batch-orchestration` workflow executes all batch steps sequentially, including the 	`ch05-refresh-bq-iceberg-tables` and `ch05-dbt-job cloud` run jobs.

```bash
gcloud workflows deploy ch05-wf-full-batch-orchestration \
  --source=chapter_05/orchestration/workflow_full_batch_orchestration.yml \
  --location=$REGION \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,REGION=$REGION

gcloud workflows deploy ch05-wf-ingest-products \
  --source=chapter_05/orchestration/workflow_ingest_products.yml \
  --location=$REGION \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,REGION=$REGION,SERVICE_ACCOUNT=$SERVICE_ACCOUNT

gcloud workflows deploy ch05-wf-ingest-orders \
  --source=chapter_05/orchestration/workflow_ingest_orders.yml \
  --location=$REGION \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,REGION=$REGION,SERVICE_ACCOUNT=$SERVICE_ACCOUNT

gcloud workflows deploy ch05-wf-ingest-customers \
  --source=chapter_05/orchestration/workflow_ingest_customers.yml \
  --location=$REGION \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,REGION=$REGION,SERVICE_ACCOUNT=$SERVICE_ACCOUNT

gcloud workflows deploy ch05-wf-ingest-live_orders \
  --source=chapter_05/orchestration/workflow_ingest_live_orders.yml \
  --location=$REGION \
  --set-env-vars=PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,REGION=$REGION,SERVICE_ACCOUNT=$SERVICE_ACCOUNT      
```

## 3 Run workflows
This can be done directly via the https://console.cloud.google.com/workflows?project=<PROJECT_ID> console or programmatically using the following command:

```bash

# Full batch pipeline (sequential - streaming not included)
gcloud workflows run ch05-wf-full-batch-orchestration --location=$REGION

# Individual batch workflows (on demand)
gcloud workflows run ch05-wf-ingest-customers --location=$REGION
gcloud workflows run ch05-wf-ingest-products --location=$REGION
gcloud workflows run ch05-wf-ingest-orders --location=$REGION

# Individual streaming workflows (on demand)
gcloud workflows run ch05-wf-ingest-live_orders --location=$REGION
```