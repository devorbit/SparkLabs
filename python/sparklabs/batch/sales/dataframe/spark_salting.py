from pyspark.sql import functions as F

from sparklabs.batch.sales.dataframe import utils


def run() -> None:
    (
        utils.spark.range(20)
        .withColumn("salt", (F.rand() * 10).cast("int"))
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
