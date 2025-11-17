# FastAPI app exposing 3 APIs & producing events to Kafka

from typing import List

from fastapi import FastAPI, HTTPException
from confluent_kafka import Producer

from .config import settings
from .models import Customer, Product, Order
from .kafka_producer import create_kafka_producer, send_event
from .data_generators import (
    sample_customers,
    sample_products,
    generate_random_order,
    generate_random_customer,
    generate_random_product,
)

app = FastAPI(
    title="Chapter 5 - Simple Data Producers",
    description=(
        "Toy e-commerce APIs that expose customers, products and orders, "
        "and produce corresponding Kafka events."
    ),
    version="1.1.0",
)

producer: Producer | None = None

# In-memory "database"
customers_db: List[Customer] = []
products_db: List[Product] = []
orders_db: List[Order] = []

# Simple ID counters (increment across calls while the app is running)
customer_counter: int = 0
product_counter: int = 0
order_counter: int = 0


@app.on_event("startup")
def startup_event() -> None:
    """
    Create the Kafka producer and initialize our in-memory data.
    """
    global producer, customers_db, products_db
    global customer_counter, product_counter, order_counter

    producer = create_kafka_producer()

    # Initialize customers and products only once
    if not customers_db:
        customers_db = sample_customers()
    if not products_db:
        products_db = sample_products()

    # Initialize counters from existing data
    customer_counter = max((c.id or 0) for c in customers_db) if customers_db else 0
    product_counter = max((p.id or 0) for p in products_db) if products_db else 0
    order_counter = max((o.id or 0) for o in orders_db) if orders_db else 0


@app.on_event("shutdown")
def shutdown_event() -> None:
    """
    Close the Kafka producer when the API shuts down.
    """
    global producer
    if producer is not None:
        producer.flush()
        producer = None


@app.get("/", tags=["root"])
def root():
    return {
        "message": "Chapter 5 producers are running. Visit /docs for the API UI."
    }


@app.get("/health", tags=["health"])
def health_check():
    """
    Simple health endpoint to verify the API is running.
    """
    return {"status": "ok"}


# ----------------------------
# Customers API + producer
# ----------------------------

@app.get("/customers", response_model=List[Customer], tags=["customers"])
def get_customers():
    """
    Return all customers currently in memory.

    This will also emit each customer to Kafka when called.
    """
    global producer, customers_db

    if producer is not None:
        for customer in customers_db:
            send_event(
                producer=producer,
                topic=settings.topic_customers,
                key=customer.id,
                value=customer.model_dump(),
            )

    return customers_db


@app.post("/customers_create_manual", response_model=Customer, status_code=201, tags=["customers"])
def create_customer(customer: Customer):
    """
    Create a new customer manually via the API.

    If the 'id' is not provided, it is assigned automatically.
    """
    global customers_db, customer_counter, producer

    # Auto-assign ID if missing
    if customer.id is None:
        customer_counter += 1
        customer.id = customer_counter
    else:
        if any(c.id == customer.id for c in customers_db):
            raise HTTPException(
                status_code=400, detail="Customer with this ID already exists"
            )
        customer_counter = max(customer_counter, customer.id)

    customers_db.append(customer)

    if producer is not None:
        send_event(
            producer=producer,
            topic=settings.topic_customers,
            key=customer.id,
            value=customer.model_dump(),
        )

    return customer


@app.post(
    "/customers_generate",
    response_model=List[Customer],
    status_code=201,
    tags=["customers"],
)
def generate_customers(count: int = 1):
    """
    Generate 'count' new random customers and emit them to Kafka.
    """
    global customers_db, customer_counter, producer

    new_customers: List[Customer] = []

    for _ in range(count):
        customer_counter += 1
        new_customer = generate_random_customer(customer_counter)
        customers_db.append(new_customer)
        new_customers.append(new_customer)

        if producer is not None:
            send_event(
                producer=producer,
                topic=settings.topic_customers,
                key=new_customer.id,
                value=new_customer.model_dump(),
            )

    return new_customers


# ----------------------------
# Products API + producer
# ----------------------------

@app.get("/products", response_model=List[Product], tags=["products"])
def get_products():
    """
    Return all products currently in memory.

    This will also emit each product to Kafka when called.
    """
    global producer, products_db

    if producer is not None:
        for product in products_db:
            send_event(
                producer=producer,
                topic=settings.topic_products,
                key=product.id,
                value=product.model_dump(),
            )

    return products_db


@app.post("/products_create_manual", response_model=Product, status_code=201, tags=["products"])
def create_product(product: Product):
    """
    Create a new product manually via the API.

    If the 'id' is not provided, it is assigned automatically.
    """
    global products_db, product_counter, producer

    if product.id is None:
        product_counter += 1
        product.id = product_counter
    else:
        if any(p.id == product.id for p in products_db):
            raise HTTPException(
                status_code=400, detail="Product with this ID already exists"
            )
        product_counter = max(product_counter, product.id)

    products_db.append(product)

    if producer is not None:
        send_event(
            producer=producer,
            topic=settings.topic_products,
            key=product.id,
            value=product.model_dump(),
        )

    return product


@app.post(
    "/products_generate",
    response_model=List[Product],
    status_code=201,
    tags=["products"],
)
def generate_products(count: int = 1):
    """
    Generate 'count' new random products and emit them to Kafka.
    """
    global products_db, product_counter, producer

    new_products: List[Product] = []

    for _ in range(count):
        product_counter += 1
        new_product = generate_random_product(product_counter)
        products_db.append(new_product)
        new_products.append(new_product)

        if producer is not None:
            send_event(
                producer=producer,
                topic=settings.topic_products,
                key=new_product.id,
                value=new_product.model_dump(),
            )

    return new_products


# ----------------------------
# Orders API + producer
# ----------------------------
@app.get(
    "/orders",
    response_model=List[Order],
    tags=["orders"],
)
def get_orders_history():
    """
    Return the full list of orders generated or created so far.
    """
    global orders_db
    return orders_db


@app.post("/orders_create_manual", response_model=Order, status_code=201, tags=["orders"])
def create_order(order: Order):
    """
    Create an order manually via the API.

    - If 'id' is missing, it's assigned automatically.
    - You must provide a consistent 'total_amount' for now.
      (You could extend this to calculate from product price.)
    """
    global orders_db, order_counter, producer

    if order.id is None:
        order_counter += 1
        order.id = order_counter
    else:
        if any(o.id == order.id for o in orders_db):
            raise HTTPException(
                status_code=400, detail="Order with this ID already exists"
            )
        order_counter = max(order_counter, order.id)

    orders_db.append(order)

    if producer is not None:
        send_event(
            producer=producer,
            topic=settings.topic_orders,
            key=order.id,
            value=order.model_dump(),
        )

    return order


@app.get(
    "/orders_generate",
    response_model=List[Order],
    tags=["orders"],
)
def generate_orders(count: int = 5):
    """
    Generate N random orders and emit them to Kafka.

    Order IDs keep increasing across calls, so each new order
    gets a unique, incremental ID for this server run.
    """
    global producer, order_counter, orders_db

    orders: List[Order] = []

    # Start from the next available ID
    start_id = order_counter + 1
    end_id = order_counter + count

    for order_id in range(start_id, end_id + 1):
        order = generate_random_order(order_id=order_id)
        orders.append(order)
        orders_db.append(order)

        if producer is not None:
            send_event(
                producer=producer,
                topic=settings.topic_orders,
                key=order.id,
                value=order.model_dump(),
            )

    # Update the global counter so next call continues from here
    order_counter = end_id

    return orders

