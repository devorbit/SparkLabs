from dataclasses import dataclass


@dataclass
class Customer:
    customer_id: int
    customer_name: str
    city: str
    age: int
    signup_date: str


@dataclass
class Order:
    order_id: int
    customer_id: int
    store_id: int
    order_date: str
    order_status: str


@dataclass
class OrderItem:
    order_item_id: int
    order_id: int
    product_id: int
    quantity: int


@dataclass
class Product:
    product_id: int
    product_name: str
    category: str
    price: float
