# Fake data generators

# src/data_generators.py

from datetime import datetime
from random import choice, randint, uniform
from typing import List

from .models import Customer, Product, Order


def sample_customers() -> List[Customer]:
    return [
        Customer(id=1, name="Alice Johnson", email="alice@example.com", country="PT"),
        Customer(id=2, name="Bruno Silva", email="bruno@example.com", country="PT"),
        Customer(id=3, name="Carla Gómez", email="carla@example.com", country="ES"),
        Customer(id=4, name="Daniel Müller", email="daniel@example.com", country="DE"),
    ]


def sample_products() -> List[Product]:
    return [
        Product(id=1, name="T-Shirt", category="Apparel", price=9.99),
        Product(id=2, name="Jeans", category="Apparel", price=39.99),
        Product(id=3, name="Sneakers", category="Footwear", price=59.90),
        Product(id=4, name="Jacket", category="Apparel", price=89.00),
    ]


def generate_random_customer(customer_id: int) -> Customer:
    first_names = ["Eva", "Peter", "Chris", "Thomas", "Patricia", "Pedro", "Rui", "Mike", "Sofia", "Ana", "Laura", "Lucas", "Joana", "Rita", "Clara"]
    last_names = ["Johnson", "Costa", "Lopes", "Ferreira", "Silva", "Gomes", "Martins", "Rodriguez", "Smith", "Brown", "Davis", "Schmidt", "Garcia", "Lopez", "Müller", "Taylor", "Williams"]
    countries = ["PT", "ES", "FR", "DE", "IT", "US", "BR", "GB", "NL", "BE", "SE", "NO", "DK", "FI", "CH", "AU"]

    first = choice(first_names)
    last = choice(last_names)
    full_name = f"{first} {last}"
    email = f"{first.lower()}.{last.lower()}_{customer_id}@example.com"

    return Customer(
        id=customer_id,
        name=full_name,
        email=email,
        country=choice(countries),
    )


def generate_random_product(product_id: int) -> Product:
    product_names = [
        ("Basic Tee", "Apparel"),
        ("Slim Jeans", "Apparel"),
        ("Running Shoes", "Footwear"),
        ("Hoodie", "Apparel"),
        ("Cap", "Accessories"),
        ("Backpack", "Accessories"),
    ]

    name, category = choice(product_names)
    base_price = uniform(5.0, 120.0)
    price = round(base_price, 2)

    return Product(
        id=product_id,
        name=name,
        category=category,
        price=price,
    )


def generate_random_order(order_id: int) -> Order:
    customers = sample_customers()
    products = sample_products()

    customer = choice(customers)
    product = choice(products)

    quantity = randint(1, 5)
    total_amount = round(product.price * quantity, 2)

    return Order(
        id=order_id,
        customer_id=customer.id,
        product_id=product.id,
        quantity=quantity,
        total_amount=total_amount,
        currency="EUR",
        created_at=datetime.utcnow(),
    )
