from pyspark.sql import functions as F

from sparklabs.batch.sales.dataset import utils


def run() -> None:
    (
        utils.customers.join(utils.orders, ["customer_id"], "inner")
        .groupBy(F.col("customer_id"))
        .agg(F.approx_count_distinct("customer_id").alias("total_orders"))
        .orderBy(F.desc("total_orders"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
