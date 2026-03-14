from pyspark.sql import functions as F

from sparklabs.batch.sales import utils


def run() -> None:
    (
        utils.order_items.join(utils.products, ["product_id"], "inner")
        .groupBy(F.col("product_id"))
        .agg(F.count("*").alias("total_orders"))
        .orderBy(F.desc("total_orders"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
