from typing import List, Optional

from fastapi import FastAPI, HTTPException

from .config import settings
from .models import Customer, Product, Order
from .data_generators import (
    sample_customers,
    sample_products,
    generate_random_order,
    generate_random_customer,
    generate_random_product,
)

from .pubsub_publisher import PubSubPublisher


app = FastAPI(
    title=settings.app_name,
    description="E-Commerce APIs that expose customers, products and orders.",
    version=settings.app_version,
)

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
    Initialize our in-memory data. No external systems involved.
    """
    global customers_db, products_db
    global customer_counter, product_counter, order_counter
    global publisher

    if not customers_db:
        customers_db = sample_customers()
    if not products_db:
        products_db = sample_products()

    customer_counter = max((c.id or 0) for c in customers_db) if customers_db else 0
    product_counter = max((p.id or 0) for p in products_db) if products_db else 0
    order_counter = max((o.id or 0) for o in orders_db) if orders_db else 0

    if settings.pubsub_enabled:
        publisher = PubSubPublisher(settings.gcp_project_id, settings.pubsub_orders_topic)



@app.get("/", tags=["root"])
def root():
    return {"message": "Chapter 5 producers are running. Visit /docs for the API UI."}


@app.get("/health", tags=["health"])
def health_check():
    return {
        "status": "ok",
        "pubsub_enabled": settings.pubsub_enabled,
        "pubsub_topic": settings.pubsub_orders_topic if settings.pubsub_enabled else None,
    }


# ----------------------------
# Customers API
# ----------------------------
@app.get("/customers", response_model=List[Customer], tags=["customers"])
def get_customers():
    return customers_db


@app.post("/customers_create_manual", response_model=Customer, status_code=201, tags=["customers"])
def create_customer(customer: Customer):
    global customers_db, customer_counter

    if customer.id is None:
        customer_counter += 1
        customer.id = customer_counter
    else:
        if any(c.id == customer.id for c in customers_db):
            raise HTTPException(status_code=400, detail="Customer with this ID already exists")
        customer_counter = max(customer_counter, customer.id)

    customers_db.append(customer)
    return customer


@app.post("/customers_generate", response_model=List[Customer], status_code=201, tags=["customers"])
def generate_customers(count: int = 1):
    global customers_db, customer_counter

    new_customers: List[Customer] = []
    for _ in range(count):
        customer_counter += 1
        new_customer = generate_random_customer(customer_counter)
        customers_db.append(new_customer)
        new_customers.append(new_customer)

    return new_customers


# ----------------------------
# Products API
# ----------------------------
@app.get("/products", response_model=List[Product], tags=["products"])
def get_products():
    return products_db


@app.post("/products_create_manual", response_model=Product, status_code=201, tags=["products"])
def create_product(product: Product):
    global products_db, product_counter

    if product.id is None:
        product_counter += 1
        product.id = product_counter
    else:
        if any(p.id == product.id for p in products_db):
            raise HTTPException(status_code=400, detail="Product with this ID already exists")
        product_counter = max(product_counter, product.id)

    products_db.append(product)
    return product


@app.post("/products_generate", response_model=List[Product], status_code=201, tags=["products"])
def generate_products(count: int = 1):
    global products_db, product_counter

    new_products: List[Product] = []
    for _ in range(count):
        product_counter += 1
        new_product = generate_random_product(product_counter)
        products_db.append(new_product)
        new_products.append(new_product)

    return new_products


# ----------------------------
# Orders API
# ----------------------------
@app.get("/orders", response_model=List[Order], tags=["orders"])
def get_orders_history():
    return orders_db


@app.post("/orders_create_manual", response_model=Order, status_code=201, tags=["orders"])
def create_order(order: Order):
    global orders_db, order_counter, customers_db, products_db, publisher

    # Referential integrity checks
    if not any(c.id == order.customer_id for c in customers_db):
        raise HTTPException(
            status_code=400,
            detail=f"Customer with id={order.customer_id} does not exist"
        )

    if not any(p.id == order.product_id for p in products_db):
        raise HTTPException(
            status_code=400,
            detail=f"Product with id={order.product_id} does not exist"
        )

    # Order ID handling
    if order.id is None:
        order_counter += 1
        order.id = order_counter
    else:
        if any(o.id == order.id for o in orders_db):
            raise HTTPException(
                status_code=400,
                detail="Order with this ID already exists"
            )
        order_counter = max(order_counter, order.id)

    # Persist order in the "database" first (only publish if persisted successfully)
    orders_db.append(order)

    # Publish to Pub/Sub if enabled (speed layer feed)
    if settings.pubsub_enabled and publisher is not None:
        publisher.publish_json(order.model_dump())

    return order


@app.get("/orders_generate", response_model=List[Order], tags=["orders"])
def generate_orders(count: int = 5):
    global order_counter, orders_db, customers_db, products_db, publisher

    if not customers_db or not products_db:
        raise HTTPException(
            status_code=400,
            detail="Cannot generate orders: customers and/or products are empty. Generate them first."
        )

    orders: List[Order] = []

    start_id = order_counter + 1
    end_id = order_counter + count

    for order_id in range(start_id, end_id + 1):
        order = generate_random_order(
            order_id=order_id,
            customers=customers_db,
            products=products_db,
        )

        # Persist orders in the "database" first (only publish if persisted successfully)
        orders.append(order)
        orders_db.append(order)

        # Publish each order event
        if settings.pubsub_enabled and publisher is not None:
            publisher.publish_json(order.model_dump())


    order_counter = end_id
    return orders
