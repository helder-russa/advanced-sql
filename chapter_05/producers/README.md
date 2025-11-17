# Producers – Setup Guide

This folder contains the **simple data producers** used in Chapter 5.  
They expose three APIs (**customers**, **products**, and **orders**) and publish events to Kafka.

Follow the steps below to install all requirements, run Kafka locally via Docker, and launch the FastAPI app.

---

## 1. Install Docker & Docker Compose (for Kafka)

We run Kafka **inside Docker** so you don’t need to install Kafka or Java manually.

### 1.1 Install Docker Desktop
Download for your OS:  
👉 https://www.docker.com/products/docker-desktop/

### 1.2 Launch Docker Desktop
Make sure Docker Desktop is running before continuing.

### 1.3 Verify installation
Open your terminal and run the following:

```bash
docker --version
docker compose version
```

### 1.4 Why Docker is used
Kafka is **not installed directly** on your machine, instead, we use an official `apache/kafka` Docker image and run it via Docker Compose.

### 1.5 About the docker-compose.yml
Inside this project, you will find:

```bash
chapter_05/producers/docker-compose.yml
```

This file is adapted from Confluent’s “Kafka on Docker” example:
👉 https://developer.confluent.io/confluent-tutorials/kafka-on-docker/

By installing it, you have:
- A **single Kafka broker**
- Running in **KRaft mode** (no ZooKeeper)
- **Auto-create topics** enabled
- No authentication / SSL which makes it simple for local testing

### 1.6 Start Kafka
From the terminal, navigate to the folder containing `docker-compose.yml`:

```bash
cd path/to/chapter_05/producers
docker compose up -d
```

### 1.7 Verify Kafka is running

```bash
docker ps
```

You should see a container named **kafka** (or similar, using the apache/kafka:latest image).

## 2. Install Python Requirements

### 2.1 Create a virtual environment (not mandatory, but recommended)
```bash
python3 -m venv .venv
source .venv/bin/activate      # Windows: .venv\Scripts\activate
```

### 2.2 Upgrade pip
Within the virtual environment, run the following:
```bash
pip install --upgrade pip
```

### 2.3 Install the producers library dependencies
```bash
pip install -r requirements.txt
```

**What each dependency does?**
- **FastAPI** — Web framework used for the APIs.
- **Uvicorn** — ASGI server that runs the FastAPI app.
- **confluent-kafka** — Official Kafka client for producing events.
- **Pydantic** — Validates request/response schemas.
- **python-dotenv** — Optional helper for environment variables.

## 3. Start the FastAPI App
⚠️ Important: Run this from the producers folder (not inside src).

From your virtual environment:
```bash
python -m uvicorn src.main:app --reload
```

You should see in you console (or similar):
```bash
INFO:     Uvicorn running on http://127.0.0.1:8000 (Press CTRL+C to quit)
```

## 4. Explore the APIs
Open:

👉 http://127.0.0.1:8000/docs -> This is the interactive Swagger UI provided by FastAPI. 

Here you can:
- View all customers, products, and orders
- Generate automatically new customers, products, and orders
- Generate random orders

- Create objects manually through the interface

Every API call will:
- Return data in JSON
- Produce a message to the corresponding Kafka topic (customers, products, orders)

Just expand any endpoint and click “Try it out”.

## Troubleshooting:

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
