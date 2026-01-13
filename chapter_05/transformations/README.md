# Chapter 05 – Transformations with dbt

This folder contains the **dbt project** responsible for transforming data from the **Bronze layer** into **Silver** and **Gold** analytical models.

The goal of this layer is not ingestion or storage, but **expressing business logic using SQL**, with strong guarantees around correctness, lineage, and documentation.

⚠️ Ensure that all CLI executions which reference local files are run from the root directory: `ÀDVANCED_SQL`

---

## Execution model
dbt can be run in two ways:

1) Local execution (recommended for development)

Used to:
- iterate on models
- write tests
- generate documentation

2) Cloud execution (used in production grade solution and orchestration examples)

A Cloud Run Job runs:
- `dbt build`
- `dbt docs generate`
- publishes docs to GCS

This is what you later orchestrate.

---

## 1. Data Layers

### Bronze (source)
- Managed outside dbt
- Stored as **Apache Iceberg tables**
- Exposed to BigQuery via **BigLake**
- Contains raw but structured data

### Silver (dbt models)
- Cleaned, typed, and standardized data
- One model per business entity
- Snapshot-aware (latest Iceberg snapshot only)
- Materialized as **views**

### Gold (dbt models)
- Business-ready analytical tables
- Dimensional models and facts
- Materialized as **tables**

## 2. Schema & Dataset Strategy

BigQuery datasets are mapped directly to data maturity levels:

| Layer  | BigQuery Dataset |
|--------|------------------|
| Bronze | `bronze`         |
| Silver | `silver`         |
| Gold   | `gold`           |

This mapping is enforced via a custom macro:

```sql
generate_schema_name()
```

### 3. Environement variables, Service Account (dbt runner) and authentication (required for both local and cloud execution)
```bash
PROJECT_ID="advance-sql-de-demo"
SA_NAME="ch05-dbt-runner-sa"
SA_DISPLAY_NAME="Chapter 05 dbt Runner Service Account"
BUCKET="advance-sql-de-bucket"
DOCS_BUCKET="advance-sql-de-bucket-dbt-docs"

gcloud iam service-accounts create ch05-dbt-runner-sa \
  --project="$PROJECT_ID" \
  --display-name="$SA_DISPLAY_NAME"
```

and grant the minimum permissions:
```bash
gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding "$PROJECT_ID" \
  --member="serviceAccount:ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor" 

gcloud projects add-iam-policy-binding "${PROJECT_ID}" \
  --member="serviceAccount:ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/bigquery.user"

gcloud storage buckets add-iam-policy-binding "gs://$BUCKET" \
  --member="serviceAccount:ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"

gcloud storage buckets add-iam-policy-binding "gs://$DOCS_BUCKET" \
  --member="serviceAccount:ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --role="roles/storage.objectUser"  
```

## 4. Local execution
### 4.1 Create a virtual environment + dbt init - only required if you want dbt run locally otherwise move to step 45.
From the `chapter_05/transformations` folder

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

#### 4.1.1 Upgrade pip
Within the virtual environment, run the following:
```bash
pip install --upgrade pip
```

#### 4.1.2 Install the producers library dependencies
```bash
pip install -r requirements.txt
```

#### 4.1.3 Run dbt init in your cli - this will create profiles.yml file required to run dbt locally, otherwise move to step 2.
```bash
dbt init
```

### 4.2 Download the Service account key generated previously
```bash
mkdir -p chapter_05/transformations/dbt/.secrets

gcloud iam service-accounts keys create \
  chapter_05/transformations/dbt/.secrets/ch05-dbt-runner-sa.json \
  --iam-account="ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

### 4.3 Update the `keyfile` inside profiles.yml
Search for `profiles.yml` and change the `keyfile`, copying the full path of your service account.

- macOS / Linyx: `~/.dbt/profiles.yml`
- Windows: `%USERPROFILE%\.dbt\profiles.yml`

`profiles.yml` example configuration:

```yaml
ch05_dbt_transformations:
  outputs:
    dev:
      dataset: bronze
      job_execution_timeout_seconds: 300
      job_retries: 1
      keyfile: full/path/to/service-account.json
      location: <REGION>
      method: service-account
      priority: interactive
      project: <PROJECT_ID>
      threads: 4
      type: bigquery
  target: dev
```
👉 Silver and Gold datasets are created automatically by dbt.

## 5. Cloud execution
### 5.1 Create a new bucket to store dbt docs
Create a separate bucket where dbt will publish the **dbt documentation**. This bucket will have public read access, which is why it is intentionally kept separate from the main bucket.

Bucket creation:
```bash
gcloud storage buckets create "gs://$DOCS_BUCKET" \
  --project="$PROJECT_ID" \
  --location="$REGION" \
  --uniform-bucket-level-access
```

Setting minimum permissions so external users can see it (not recommended for production grade, but enough for demo/testing)
```bash
gcloud storage buckets add-iam-policy-binding "gs://$DOCS_BUCKET" \
  --member="allUsers" \
  --role="roles/storage.objectViewer"
```

### 5.2. Deploy dbt job
This job is responsible for executing dbt commands and updating all required artifacts, including models, tests, and documentation.

```bash
gcloud run jobs deploy ch05-dbt-job \
  --project="${PROJECT_ID}" \
  --region="${REGION}" \
  --source="chapter_05/transformations/dbt/ch05_dbt_transformations" \
  --service-account="ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com" \
  --set-env-vars="PROJECT_ID=$PROJECT_ID,BUCKET=$BUCKET,DOCS_BUCKET=$DOCS_BUCKET,DOCS_PREFIX=dbt_docs"
  --task-timeout 1800 \
  --max-retries 0
```

Job execution:
```bash
gcloud run jobs execute ch05-dbt-job \
  --project="${PROJECT_ID}" \
  --region="${REGION}"
```

Confirm the documentation was correctly generated by opening `gs://$DOCS_BUCKET/dbt_docs/index.html` public URL.

