FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates curl git \
  && rm -rf /var/lib/apt/lists/*

# Install gsutil (via Google Cloud SDK)
RUN curl -sSL https://sdk.cloud.google.com | bash
ENV PATH="/root/google-cloud-sdk/bin:${PATH}"

# Python deps (keep requirements.txt in this same folder)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Copy the dbt project (everything in this folder)
COPY . /app

# Inline entrypoint
RUN printf '%s\n' \
'#!/usr/bin/env bash' \
'set -euo pipefail' \
': "${PROJECT_ID:?PROJECT_ID is required}"' \
': "${DOCS_BUCKET:?DOCS_BUCKET is required}"' \
': "${BUCKET:?BUCKET is required}"' \
': "${DOCS_PREFIX:?DOCS_PREFIX is required}"' \
'' \
'DBT_PROJECT_DIR="/app"' \
'DBT_PROFILES_DIR="/app"' \
'' \
'echo "[dbt] project_dir=${DBT_PROJECT_DIR}"' \
'echo "[dbt] profiles_dir=${DBT_PROFILES_DIR}"' \
'' \
'dbt deps --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR"' \
'dbt build --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR"' \
'dbt docs generate --project-dir "$DBT_PROJECT_DIR" --profiles-dir "$DBT_PROFILES_DIR"' \
'' \
'echo "[gcs] publish target/ to gs://${DOCS_BUCKET}/${DOCS_PREFIX}/"' \
'gsutil -m rsync -r -d "${DBT_PROJECT_DIR}/target" "gs://${DOCS_BUCKET}/${DOCS_PREFIX}/"' \
'' \
'echo "[done] docs URL: https://storage.googleapis.com/${DOCS_BUCKET}/${DOCS_PREFIX}/index.html"' \
> /app/entrypoint.sh \
&& chmod +x /app/entrypoint.sh

ENTRYPOINT ["/app/entrypoint.sh"]
