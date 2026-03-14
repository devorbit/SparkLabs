from pyspark.sql import Window
from pyspark.sql import functions as F

from sparklabs.batch.sales.dataset import utils


def run() -> None:
    window_spec = Window.orderBy(F.desc("category_count"))

    (
        utils.order_items.groupBy(F.col("product_id"))
        .agg(F.count("*").alias("order_count"))
        .join(utils.products, ["product_id"], "inner")
        .groupBy(F.col("category"))
        .agg(F.count("*").alias("category_count"))
        .withColumn("category_rank", F.dense_rank().over(window_spec))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
