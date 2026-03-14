from pyspark.sql import functions as F

from sparklabs.batch.sales import utils


def run() -> None:
    duplicate_ids = (
        utils.customers.groupBy("customer_id")
        .count()
        .where(F.col("count") > 1)
    )
    duplicate_count = duplicate_ids.count()
    if duplicate_count == 0:
        print("PASS: customer_id is unique")
    else:
        print(f"FAIL: found {duplicate_count} duplicate customer_id values")
        duplicate_ids.show(truncate=False)


if __name__ == "__main__":
    run()
