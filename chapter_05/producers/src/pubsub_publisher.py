import json
import logging
from typing import Any, Dict, Optional

from google.cloud import pubsub_v1

logger = logging.getLogger(__name__)


class PubSubPublisher:
    """
    Google Pub/Sub publishing.

    - Uses Application Default Credentials (ADC):
      - On Cloud Run: the service account identity of the service
      - Locally: your gcloud ADC or service account key depending on your setup
    """

    def __init__(self, project_id: str, topic_id: str):
        self.project_id = project_id
        self.topic_id = topic_id
        self.publisher = pubsub_v1.PublisherClient()
        self.topic_path = self.publisher.topic_path(project_id, topic_id)

    def publish_json(self, payload: Dict[str, Any], attributes: Optional[Dict[str, str]] = None) -> str:
        """
        Publish a JSON payload as UTF-8 bytes. Returns the Pub/Sub message ID.
        """
        data = json.dumps(payload, default=str).encode("utf-8")
        future = self.publisher.publish(self.topic_path, data=data, **(attributes or {}))
        msg_id = future.result()
        logger.info("Published message to Pub/Sub topic=%s msg_id=%s", self.topic_id, msg_id)
        return msg_id
