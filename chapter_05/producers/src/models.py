# Pydantic models for Customer/Product/Order

from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


class Customer(BaseModel):
    id: Optional[int] = Field(
        None, description="Unique customer ID (assigned automatically if missing)"
    )
    name: str
    email: str
    country: str


class Product(BaseModel):
    id: Optional[int] = Field(
        None, description="Unique product ID (assigned automatically if missing)"
    )
    name: str
    category: str
    price: float


class Order(BaseModel):
    id: Optional[int] = Field(
        None, description="Unique order ID (assigned automatically if missing)"
    )
    customer_id: int
    product_id: int
    quantity: int = 1
    total_amount: float
    currency: str = "EUR"
    created_at: datetime
