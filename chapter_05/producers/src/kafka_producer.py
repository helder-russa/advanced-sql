# Helper to create a Kafka producer

# src/kafka_producer.py

import json
from typing import Any, Dict, Optional

from confluent_kafka import Producer
from .config import settings


def create_kafka_producer() -> Producer:
    """
    Create a Confluent Kafka Producer instance.

    For local development we only need the bootstrap.servers config.
    """
    conf = {
        "bootstrap.servers": settings.kafka_bootstrap_servers,
    }
    return Producer(conf)


def _delivery_report(err, msg) -> None:
    """
    Callback called once for each message to indicate delivery result.

    In a real system you would log errors instead of printing, but for
    this book we keep it simple and visible.
    """
    if err is not None:
        print(f"Delivery failed for record {msg.key()}: {err}")
    # else:
    #     print(f"Record produced to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")


def send_event(
    producer: Producer,
    topic: str,
    key: Any,
    value: Dict[str, Any],
    flush: bool = True,
) -> None:
    """
    Send a single event to Kafka.

    We serialize the value as JSON and the key as a string.
    For simplicity we optionally flush after each message so that
    beginners see messages immediately.
    """
    producer.produce(
        topic=topic,
        key=str(key) if key is not None else None,
        value=json.dumps(value, default=str),
        callback=_delivery_report,
    )

    # In real-world systems you usually do NOT flush on every message.
    # Here we do it so that examples are deterministic and easy to test.
    if flush:
        producer.flush()
