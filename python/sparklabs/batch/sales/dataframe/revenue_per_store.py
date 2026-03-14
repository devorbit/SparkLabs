from pyspark.sql import functions as F

from sparklabs.batch.sales.dataframe import utils


def run() -> None:
    revenue_per_order = (
        utils.order_items.join(utils.products, ["product_id"], "inner")
        .withColumn("revenue", F.col("price") * F.col("quantity"))
        .groupBy(F.col("order_id"))
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("total_orders"),
        )
        .orderBy(F.desc("total_revenue"))
    )

    (
        utils.orders.join(revenue_per_order, ["order_id"], "inner")
        .join(utils.stores, ["store_id"], "inner")
        .groupBy(F.col("store_id"))
        .agg(
            F.sum("total_revenue").alias("store_revenue"),
            F.count("*").alias("total_store_orders"),
        )
        .orderBy(F.desc("store_revenue"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
