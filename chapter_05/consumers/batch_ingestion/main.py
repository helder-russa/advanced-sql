import os
import json
import time
from datetime import datetime, timezone
from typing import Any, Dict, List

import requests
from google.cloud import storage


# ------------------------
# Helpers
# ------------------------

def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def snapshot_dt_ts(now: datetime) -> tuple[str, str]:
    return (
        now.strftime("%Y-%m-%d"),
        now.strftime("%Y%m%dT%H%M%SZ"),
    )


def env_required(name: str) -> str:
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required env var: {name}")
    return value


# ------------------------
# IO
# ------------------------

def fetch_list(
    base_url: str,
    entity: str,
    timeout_s: int,
) -> List[Dict[str, Any]]:
    """
    Expects:
      GET {base_url}/{entity}
      → JSON array
    """
    url = f"{base_url.rstrip('/')}/{entity}"
    resp = requests.get(url, timeout=timeout_s)
    resp.raise_for_status()

    data = resp.json()
    if not isinstance(data, list):
        raise ValueError(f"Expected list from {url}, got {type(data)}")

    return data


def enrich_rows(
    rows: List[Dict[str, Any]],
    entity: str,
    ingest_time_utc: str,
) -> List[Dict[str, Any]]:
    """
    Minimal enrichment.
    Spark owns semantics & modeling later.
    """
    out: List[Dict[str, Any]] = []

    for row in rows:
        r = dict(row)

        r["_ingest_time_utc"] = ingest_time_utc
        r["_ingest_type"] = "batch"
        r["_entity"] = entity

        # Defaults to keep downstream Spark strict but resilient
        if entity == "orders":
            r.setdefault("status", "created")
            r.setdefault("source", "batch_snapshot")

        out.append(r)

    return out


def write_ndjson(
    bucket_name: str,
    object_path: str,
    rows: List[Dict[str, Any]],
) -> None:
    client = storage.Client()
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    payload = (
        "\n".join(
            json.dumps(row, separators=(",", ":"), ensure_ascii=False)
            for row in rows
        )
        + "\n"
    )

    blob.upload_from_string(
        payload,
        content_type="application/x-ndjson",
    )


# ------------------------
# Main
# ------------------------

def main() -> None:
    # Required
    entity = os.getenv("ENTITY", "").strip()
    if entity not in {"customers", "products", "orders"}:
        raise RuntimeError(
            "ENTITY must be one of: customers | products | orders"
        )

    bucket = env_required("BUCKET")
    api_base_url = env_required("API_BASE_URL")

    # Optional
    gcs_prefix = os.getenv("GCS_PREFIX", "landing_zone/batch")
    http_timeout = int(os.getenv("HTTP_TIMEOUT", "30"))

    now = utc_now()
    snapshot_dt, snapshot_ts = snapshot_dt_ts(now)
    ingest_time_utc = now.isoformat()

    print(
        f"[batch_ingestion] "
        f"entity={entity} "
        f"bucket={bucket} "
        f"snapshot_dt={snapshot_dt} "
        f"snapshot_ts={snapshot_ts}"
    )

    # 1. Fetch
    rows = fetch_list(
        base_url=api_base_url,
        entity=entity,
        timeout_s=http_timeout,
    )

    print(f"[batch_ingestion] fetched_rows={len(rows)}")

    # 2. Enrich
    rows = enrich_rows(
        rows=rows,
        entity=entity,
        ingest_time_utc=ingest_time_utc,
    )

    # 3. Write to GCS (landing zone)
    object_path = (
        f"{gcs_prefix.rstrip('/')}/{entity}/"
        f"snapshot_dt={snapshot_dt}/"
        f"snapshot_ts={snapshot_ts}/"
        f"{entity}.ndjson"
    )

    write_ndjson(
        bucket_name=bucket,
        object_path=object_path,
        rows=rows,
    )

    print(
        f"[batch_ingestion] wrote {len(rows)} rows → "
        f"gs://{bucket}/{object_path}"
    )

    # Small sleep to make logs readable in Cloud Run
    time.sleep(0.2)

    print("[batch_ingestion] done")


if __name__ == "__main__":
    main()
