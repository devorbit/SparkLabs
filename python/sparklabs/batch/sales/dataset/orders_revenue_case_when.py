from pyspark.sql import functions as F

from sparklabs.batch.sales.dataset import utils


def run() -> None:
    (
        utils.order_items.join(utils.orders, ["order_id"], "inner")
        .join(utils.products, ["product_id"], "inner")
        .join(utils.promotions, ["category"], "left")
        .withColumn("gross_revenue", F.col("price") * F.col("quantity"))
        .withColumn(
            "discount_pct",
            F.when(
                F.to_date(F.col("order_date")).between(
                    F.to_date(F.col("start_date")), F.to_date(F.col("end_date"))
                ),
                F.coalesce(F.col("discount_pct"), F.lit(0)),
            ).otherwise(F.lit(0)),
        )
        .withColumn(
            "discount_value", F.col("gross_revenue") * F.col("discount_pct") / 100
        )
        .withColumn("net_revenue", F.col("gross_revenue") - F.col("discount_value"))
        .groupBy(F.col("order_id"))
        .agg(
            F.round(F.sum("gross_revenue"), 2).alias("gross_revenue"),
            F.round(F.sum("discount_value"), 2).alias("discount_value"),
            F.round(F.sum("net_revenue"), 2).alias("net_revenue"),
        )
        .orderBy(F.desc("net_revenue"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
