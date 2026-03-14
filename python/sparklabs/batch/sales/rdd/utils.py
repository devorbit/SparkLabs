from pathlib import Path

from pyspark.sql import SparkSession


PROJECT_ROOT = Path(__file__).resolve().parents[5]
SALES_DATA_ROOT = PROJECT_ROOT / "data" / "sales"

spark = SparkSession.builder.appName("SparkLabsPythonRDD").master("local[*]").getOrCreate()
sc = spark.sparkContext


def _rows(file_name: str):
    lines = sc.textFile(str(SALES_DATA_ROOT / file_name))
    header = lines.first()
    return lines.filter(lambda line: line != header).map(lambda line: [part.strip() for part in line.split(",")])


customers = _rows("customers.csv").map(
    lambda row: {
        "customer_id": int(row[0]),
        "first_name": row[1],
        "last_name": row[2],
        "city": row[3],
        "signup_date": row[4],
        "segment": row[5],
    }
)
deliveries = _rows("deliveries.csv").map(
    lambda row: {
        "delivery_id": int(row[0]),
        "order_id": int(row[1]),
        "ship_date": row[2],
        "delivery_date": row[3],
        "delivery_status": row[4],
    }
)
events = _rows("events_stream.csv").map(
    lambda row: {
        "event_id": int(row[0]),
        "order_id": int(row[1]),
        "event_time": row[2],
        "ingestion_time": row[3],
        "event_type": row[4],
    }
)
inventory = _rows("inventory.csv").map(
    lambda row: {
        "inventory_id": int(row[0]),
        "store_id": int(row[1]),
        "product_id": int(row[2]),
        "stock_level": int(row[3]),
        "snapshot_date": row[4],
    }
)
order_items = _rows("order_items.csv").map(
    lambda row: {
        "order_item_id": int(row[0]),
        "order_id": int(row[1]),
        "product_id": int(row[2]),
        "quantity": int(row[3]),
    }
)
orders = _rows("orders.csv").map(
    lambda row: {
        "order_id": int(row[0]),
        "customer_id": int(row[1]),
        "store_id": int(row[2]),
        "sales_rep_id": int(row[3]),
        "order_date": row[4],
    }
)
products = _rows("products.csv").map(
    lambda row: {
        "product_id": int(row[0]),
        "product_name": row[1],
        "category": row[2],
        "price": float(row[3]),
    }
)
promotions = _rows("promotions.csv").map(
    lambda row: {
        "promotion_id": int(row[0]),
        "category": row[1],
        "discount_pct": float(row[2]),
        "start_date": row[3],
        "end_date": row[4],
    }
)
returns = _rows("returns.csv").map(
    lambda row: {
        "return_id": int(row[0]),
        "order_item_id": int(row[1]),
        "return_reason": row[2],
        "return_date": row[3],
    }
)
sales_reps = _rows("sales_reps.csv").map(
    lambda row: {
        "sales_rep_id": int(row[0]),
        "rep_name": row[1],
        "region": row[2],
    }
)
stores = _rows("stores.csv").map(
    lambda row: {
        "store_id": int(row[0]),
        "store_city": row[1],
        "store_type": row[2],
    }
)
