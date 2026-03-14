from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F


PROJECT_ROOT = Path(__file__).resolve().parents[5]
SALES_DATA_ROOT = PROJECT_ROOT / "data" / "sales"


def _sales_path(file_name: str) -> str:
    return str(SALES_DATA_ROOT / file_name)


spark = (
    SparkSession.builder.appName("SparkLabsPython").master("local[*]").getOrCreate()
)

reader = (
    spark.read.format("csv").option("header", "true").option("inferSchema", "true")
)

customers = reader.load(_sales_path("customers.csv"))
deliveries = reader.load(_sales_path("deliveries.csv"))
events = reader.load(_sales_path("events_stream.csv"))
inventory = reader.load(_sales_path("inventory.csv"))
order_items = reader.load(_sales_path("order_items.csv"))
orders = reader.load(_sales_path("orders.csv"))
products = reader.load(_sales_path("products.csv"))
promotions = reader.load(_sales_path("promotions.csv"))
returns = reader.load(_sales_path("returns.csv"))
sales_reps = reader.load(_sales_path("sales_reps.csv"))
stores = reader.load(_sales_path("stores.csv"))

print(f"Total order count: {orders.count()}")

store_orders = (
    orders.groupBy(F.col("store_id"))
    .agg(F.count("order_id").alias("order_count"))
    .where((F.col("order_count") > 250) & (F.col("order_count") < 300))
    .orderBy(F.desc("order_count"))
    .selectExpr("store_id", "order_count")
)


def register_temp_views() -> None:
    for view_name, dataframe in {
        "customers": customers,
        "deliveries": deliveries,
        "events": events,
        "inventory": inventory,
        "order_items": order_items,
        "orders": orders,
        "products": products,
        "promotions": promotions,
        "returns": returns,
        "sales_reps": sales_reps,
        "stores": stores,
    }.items():
        dataframe.createOrReplaceTempView(view_name)
