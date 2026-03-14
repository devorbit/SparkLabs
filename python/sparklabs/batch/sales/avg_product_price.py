from pyspark.sql import functions as F

from sparklabs.batch.sales import utils


def run() -> None:
    (
        utils.products.groupBy(F.col("category"))
        .agg(F.round(F.avg("price")).alias("avg_price"))
        .orderBy(F.desc("avg_price"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
