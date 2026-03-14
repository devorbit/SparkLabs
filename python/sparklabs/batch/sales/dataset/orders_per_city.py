from pyspark.sql import functions as F

from sparklabs.batch.sales.dataset import utils


def run() -> None:
    (
        utils.orders.join(utils.customers, ["customer_id"], "inner")
        .groupBy(F.col("city"))
        .agg(F.count("*").alias("total_orders_per_city"))
        .orderBy(F.desc("total_orders_per_city"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
