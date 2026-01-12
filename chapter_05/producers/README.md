# Chapter 05 - Producers – Setup Guide

This folder contains the **simple data producers** used in Chapter 5 to simulate a simple ratail operational database.  
They expose three APIs (`customers`, `products`, and `orders`) and publish `orders` events to PUB/SUB from Google Cloud.

ℹ️  All bash commands presented in this README file are to be executed in your local terminal.

ℹ️  There's a pre-assumption that all GCP services and APIs, like Storage, BigQuery, Pub/Sub, etc, are already enabled, as well as your own service account with the necessary permissions. Please refer to the book to see how all these setup was made.

---

## How this is meant to be used

There are two distinct usage modes, and it is important not to confuse them.

1) **Local exploration (optional, learning-oriented)**

Running the producers locally allows you to:
- inspect payloads
- understand schemas
- experiment with Swagger
- manually generate test data

This is only for understanding and validation.
> Data generated locally will **not** be ingested into GCS, Pub/Sub, or BigQuery.

2) **Cloud execution (required for the pipeline)**
For the end-to-end pipeline described in Chapter 5, the producers must run on Cloud Run so that:
- Pub/Sub can receive streaming events
- batch ingestion jobs can reach the APIs
- everything runs in managed infrastructure

---

## 1. Local execution (optional)
Use this only if you want to explore the APIs.

### 1.1 Install Python Requirements

#### 1.1.1 Create a virtual environment (not mandatory, but recommended)
```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

### 1.1.2 Upgrade pip
Within the virtual environment, run the following:
```bash
pip install --upgrade pip
```

### 1.1.3 Install the producers library dependencies
```bash
pip install -r requirements.txt
```

**What each dependency does?**
- **FastAPI** — Web framework used for the APIs.
- **Uvicorn** — ASGI server that runs the FastAPI app.
- **Pydantic** — Validates request/response schemas.
- **python-dotenv** — Optional helper for environment variables.

### 1.2 Start the FastAPI App
⚠️ Important: Run this from the producers folder (not inside src).

From your virtual environment:
```bash
python -m uvicorn src.main:app --reload
```

You should see in your console (or similar):
```bash
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

### 1.3 Explore the APIs
Open:

🔗 http://127.0.0.1:8000/docs -> This is the interactive Swagger UI provided by FastAPI. 

Here you can:
- View all customers, products, and orders
- Generate automatically new customers, products, and orders
- Generate random orders

- Create objects manually through the interface

Every API call will:
- Return data in JSON
- Produce a message to the corresponding Kafka topic (customers, products, orders)

Just expand any endpoint and click “Try it out”.

## 2. Cloud deployment (required)
This is the core setup used by the rest of Chapter 5.

### 2.1 Deploying the Producers to Cloud Run - important for end to end solution
Deploy the producers API to Cloud Run. This is required so that other managed services (Pub/Sub, Dataproc) can reach the API and we can build the full end-to-end pipeline.

From the `chapter_05/` folder, run:

```bash
gcloud auth activate-service-account --key-file=REPLACE-BY-YOUR-SA-KEY.json
gcloud config set project advance-sql-de-demo

gcloud run deploy ch05-producers-api \
  --source . \
  --region europe-west1 \
  --allow-unauthenticated
  ```

⚠️ By using `--allow-unauthenticated` (the simplest option for a tutorial), you are exposing the API, and therefore Swagger, publicly.
This means anyone with the URL could generate data. For a production setup, you would restrict access using IAM or identity-aware proxy (IAP). For this tutorial, we keep it simple.

Once the deployment completes, copy the Service URL and open it in your browser. It should look similar to:
🔗 https://ch05-producers-api-xxxxx.a.run.app/docs

### 2.2 Setting up Pub/Sub (speed layer entry point) - topic creation + subscription test
Next, we create the Pub/Sub infrastructure that will receive order events emitted by the producers.

#### 2.2.1 Create the topic and a debug subscription
```bash
gcloud pubsub topics create orders

gcloud pubsub subscriptions create orders-debug-sub \
  --topic=orders
```

ℹ️ The `orders-debug-sub` subscription is created only for validation and debugging. It allows us to manually inspect messages and confirm that orders are being published correctly. This will not be the final subscription used by the streaming pipeline.

#### 2.2.2 Verify creation
Check if the topic exists:
```bash
gcloud pubsub topics list
```

Check if the subscription exists:
```bash
gcloud pubsub subscriptions list
```

If both lists are with the topic and subscription, we can continue to the next configuration step.

#### 2.2.3 Grant Cloud Run permission to publish to Pub/Sub
Your Cloud Run service runs using a service account identity. That service account must be explicitly allowed to publish messages to Pub/Sub.

First, retrieve the service account used by the Cloud Run service:
```bash
gcloud run services describe ch05-producers-api \
  --region europe-west1 \
  --format='value(spec.template.spec.serviceAccountName)'
```

Store it in a variable and grant the required role:
```bash
SA_EMAIL="REPLACE-BY-YOUR-SA-FROM-PREVIOUS-BATCH-COMMAND"

gcloud projects add-iam-policy-binding advance-sql-de-demo \
  --member="serviceAccount:${SA_EMAIL}" \
  --role="roles/pubsub.publisher"
```

#### 2.2.4 Redeploy Cloud Run with Pub/Sub configuration

Now redeploy the service, this time enabling Pub/Sub publishing via environment variables.

From the `chapter_05/` folder:
```bash
gcloud run deploy ch05-producers-api \
  --source . \
  --region europe-west1 \
  --allow-unauthenticated \
  --set-env-vars GCP_PROJECT_ID=advance-sql-de-demo,PUBSUB_ORDERS_TOPIC=orders,PUBSUB_ENABLED=true
```

This deployment:
- keeps the same API and Swagger UI
- enables order event publication to Pub/Sub
- does not change any local development behavior

#### 2.2.5 Verify end-to-end event emission
1. Open Cloud Run Swagger `/docs`
2. Generate customers/products
3. Generate orders

Then, if you were able to generate everything, let's pull some messages from pubsub to make sure we are sending correctly the events to there:
```bash
gcloud pubsub subscriptions pull orders-debug-sub --limit=5 --auto-ack
```

ℹ️ As soon as a message is acknowledged, it is removed from the subscription and will not be delivered again for that subscription. This is expected behavior and matches the at-least-once delivery semantics of Pub/Sub.


## Troubleshooting

### **Port already in use**
If you receive the error:
```bash
[Errno 48] Address already in use
```
It means another process is already using port 8000 (the default Uvicorn port).

You can fix this in two ways:

#### Option 1 — Kill the process using port 8000
Find the process:
```bash
lsof -i :8000
```

This will output something like:
```bash
uvicorn   123456   youruser   ...
```

or depending you are running

```bash
Python   123456   youruser   ...
```

Kill the process using its PID:

```bash
kill -9 123456
```

(Replace **123456** with the actual PID shown in your terminal.)

#### Option 2 — Run Uvicorn on a different port
```bash
python -m uvicorn src.main:app --reload --port 8001
```

Then open:

```bash
http://127.0.0.1:8001/docs
```

### **Uvicorn is running outside the virtual environment**
If you see an error such as:

```bash
ModuleNotFoundError: No module named 'confluent_kafka'
```

even though you already installed the dependencies inside your virtual environment, the issue may be that **Uvicorn is being executed from the global Python installation**, not from your virtual environment.

To verify this, check:
```bash
which python
which pip
which uvicorn
```

If you see something like:
```bash
python  → /your/project/.venv/bin/python
pip     → /your/project/.venv/bin/pip
uvicorn → /Library/Frameworks/Python.framework/... (GLOBAL ❌)
```

Then Uvicorn is **not** coming from the venv and therefore cannot see the packages installed there.

#### Fix: Run Uvicorn using the venv Python

##### For that, refresh your shell to pick the correct Uvicorn
If you want the `uvicorn` command itself to resolve to the venv version:
```bash
deactivate
source .venv/bin/activate
```

Then check again:
```bash
which uvicorn
```

You should now see something like:
```bash
/your/project/.venv/bin/uvicorn
```

##### Optional - if the previous solution doesn't work - you can also run Uvicorn using the venv Python
```bash
python -m uvicorn src.main:app --reload
```

This forces Uvicorn to run inside the active virtual environment and guarantees that all imports are found.
