# Chapter 05 – Transformations with dbt

This folder contains the **dbt project** responsible for transforming data from the **Bronze layer** into **Silver** and **Gold** analytical models.

The goal of this layer is not ingestion or storage, but **expressing business logic using SQL**, with strong guarantees around correctness, lineage, and documentation.

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

## 3. Create a virtual environment  + dbt init
From the `chapter_05/transformations` folder

```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

### 1.2 Upgrade pip
Within the virtual environment, run the following:
```bash
pip install --upgrade pip
```

### 1.3 Install the producers library dependencies
```bash
pip install -r requirements.txt
```

### 1.4 Run dbt init in your cli - this will create profiles.yml file required to run dbt locally
```bash
dbt init
```

### 2. Service Account (dbt runner) and authentication
```bash
PROJECT_ID="advance-sql-de-demo"
SA_NAME="ch05-dbt-runner-sa"
SA_DISPLAY_NAME="Chapter 05 dbt Runner Service Account"

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
```

### 2.1 Create and download the Service account key
```bash
mkdir -p chapter_05/transformations/dbt/.secrets

gcloud iam service-accounts keys create \
  chapter_05/transformations/dbt/.secrets/ch05-dbt-runner-sa.json \
  --iam-account="ch05-dbt-runner-sa@$PROJECT_ID.iam.gserviceaccount.com"
```

### 2.2 Update the `keyfile` inside profiles.yml
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

### 3. Run models
```bash
dbt run
```

### 4. Run tests
```bash
dbt test
```

### 5. Generate documentation
```bash
dbt docs generate
dbt docs serve
```

This last commands provides:
- Model documentation
- Column descriptions
- Full lineage graph