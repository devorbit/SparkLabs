from sparklabs.batch.sales.dataset import utils


def run() -> None:
    (
        utils.spark.range(20)
        .rdd.map(lambda row: (row["id"], int(__import__("random").random() * 10)))
        .toDF(["id", "salt"])
        .show(truncate=False)
    )


if __name__ == "__main__":
    run()
