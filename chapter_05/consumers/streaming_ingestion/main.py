import os
import json
import uuid
from datetime import datetime, timezone

from google.cloud import pubsub_v1
from google.cloud import storage
from google.api_core.exceptions import DeadlineExceeded, ServiceUnavailable, InternalServerError



PROJECT_ID = os.getenv("PROJECT_ID", "advance-sql-de-demo")
SUBSCRIPTION_ID = os.getenv("SUBSCRIPTION_ID", "orders-to-gcs-sub")
BUCKET = os.getenv("BUCKET", "advance-sql-de-bucket")
GCS_PREFIX = os.getenv("GCS_PREFIX", "landing_zone/streaming/orders")

# Tuning knobs
MAX_MESSAGES = int(os.getenv("MAX_MESSAGES", "500"))
MAX_LOOPS = int(os.getenv("MAX_LOOPS", "50"))  # safety stop for demos
PULL_TIMEOUT = float(os.getenv("PULL_TIMEOUT", "10"))  # seconds


def utc_partition_prefix(dt: datetime) -> str:
    # landing_zone/streaming/orders/ingest_dt=YYYY-MM-DD/ingest_hr=HH/
    return f"{GCS_PREFIX}/ingest_dt={dt:%Y-%m-%d}/ingest_hr={dt:%H}/"


def write_jsonl_to_gcs(storage_client: storage.Client, bucket_name: str, object_path: str, records: list[dict]) -> None:
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(object_path)

    # JSON Lines (one JSON object per line)
    payload = "\n".join(json.dumps(r, separators=(",", ":"), ensure_ascii=False) for r in records) + "\n"
    blob.upload_from_string(payload, content_type="application/x-ndjson")


def main():
    subscriber = pubsub_v1.SubscriberClient()
    storage_client = storage.Client()

    subscription_path = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    loops = 0
    total_acked = 0
    total_written = 0

    while loops < MAX_LOOPS:
        loops += 1

        try:
            response = subscriber.pull(
                request={
                    "subscription": subscription_path,
                    "max_messages": MAX_MESSAGES,
                },
                timeout=PULL_TIMEOUT,
            )
        except DeadlineExceeded:
            # Normal in long-poll / low-traffic: treat as "no messages right now"
            print("Pull timed out (DeadlineExceeded). No messages available right now; exiting cleanly.")
            break
        except (ServiceUnavailable, InternalServerError) as e:
            # Transient infra hiccups; retry next loop
            print(f"Transient Pub/Sub error ({type(e).__name__}). Retrying next loop: {e}")
            continue

        if not response.received_messages:
            # No more data; exit cleanly
            break

        now = datetime.now(timezone.utc)
        prefix = utc_partition_prefix(now)
        filename = f"part-{uuid.uuid4().hex}.jsonl"
        object_path = prefix + filename

        records = []
        ack_ids = []

        for rm in response.received_messages:
            ack_ids.append(rm.ack_id)
            data_bytes = rm.message.data

            try:
                record = json.loads(data_bytes.decode("utf-8"))
            except Exception:
                # If something isn't JSON, wrap it so we never lose the payload
                record = {
                    "_corrupt_record": data_bytes.decode("utf-8", errors="replace"),
                    "_ingest_error": "non_json_payload",
                }

            # Optional metadata that helps later debugging
            record["_pubsub_message_id"] = rm.message.message_id
            record["_pubsub_publish_time"] = rm.message.publish_time.isoformat() if rm.message.publish_time else None
            record["_ingest_time_utc"] = now.isoformat()

            records.append(record)

        # Write first, ACK after successful write
        write_jsonl_to_gcs(storage_client, BUCKET, object_path, records)
        total_written += len(records)

        subscriber.acknowledge(
            request={
                "subscription": subscription_path,
                "ack_ids": ack_ids,
            }
        )
        total_acked += len(ack_ids)

        print(f"Wrote {len(records)} records to gs://{BUCKET}/{object_path} and ACKed {len(ack_ids)} messages.")

    print(f"Done. total_written={total_written}, total_acked={total_acked}, loops={loops}")


if __name__ == "__main__":
    main()