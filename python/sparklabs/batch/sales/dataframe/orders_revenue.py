from pyspark.sql import functions as F

from sparklabs.batch.sales.dataframe import utils


def run() -> None:
    (
        utils.order_items.join(utils.products, ["product_id"], "inner")
        .withColumn("revenue", F.col("price") * F.col("quantity"))
        .groupBy(F.col("order_id"))
        .agg(
            F.sum("revenue").alias("total_revenue"),
            F.count("*").alias("total_orders"),
        )
        .orderBy(F.desc("total_revenue"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
