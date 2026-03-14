package io.devorbit.sparklabs.batch.sales.dataset

case class Customer(customer_id: Int, first_name: String, last_name: String, city: String, signup_date: String, segment: String)
case class Delivery(delivery_id: Int, order_id: Int, ship_date: String, delivery_date: String, delivery_status: String)
case class Event(event_id: Int, order_id: Int, event_time: String, ingestion_time: String, event_type: String)
case class Inventory(inventory_id: Int, store_id: Int, product_id: Int, stock_level: Int, snapshot_date: String)
case class OrderItem(order_item_id: Int, order_id: Int, product_id: Int, quantity: Int)
case class Order(order_id: Int, customer_id: Int, store_id: Int, sales_rep_id: Int, order_date: String)
case class Product(product_id: Int, product_name: String, category: String, price: Double)
case class Promotion(promotion_id: Int, category: String, discount_pct: Double, start_date: String, end_date: String)
case class Return(return_id: Int, order_item_id: Int, return_reason: String, return_date: String)
case class SalesRep(sales_rep_id: Int, rep_name: String, region: String)
case class Store(store_id: Int, store_city: String, store_type: String)
