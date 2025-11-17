# Settings: Kafka host, topics, etc.

from pydantic import BaseModel


class Settings(BaseModel):
    kafka_bootstrap_servers: str = "localhost:9092"
    topic_customers: str = "customers"
    topic_products: str = "products"
    topic_orders: str = "orders"


settings = Settings()
