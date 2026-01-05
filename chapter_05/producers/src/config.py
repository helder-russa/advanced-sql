# Settings: Kafka host, topics, etc.

import os
from pydantic import BaseModel


class Settings(BaseModel):
    app_name: str = "Advanced SQL - Chapter 5, Simple Data Producers"
    app_version: str = "3.0.0"

    # GCP / PubSub, if no env vars are set, use these defaults
    gcp_project_id: str = os.getenv("GCP_PROJECT_ID", "advance-sql-de-demo") # Replace advance-sql-de-demowith your GCP project ID
    pubsub_orders_topic: str = os.getenv("PUBSUB_ORDERS_TOPIC", "orders")

    # Feature flag: allow you to toggle publishing on/off without code changes
    pubsub_enabled: bool = os.getenv("PUBSUB_ENABLED", "true").lower() in ("1", "true", "yes")


settings = Settings()
