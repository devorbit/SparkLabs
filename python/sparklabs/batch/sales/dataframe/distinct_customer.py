from pyspark.sql import functions as F

from sparklabs.batch.sales.dataframe import utils


def run() -> None:
    distinct_customer_count = utils.orders.select(F.col("customer_id")).count()
    expensive_product = utils.products.orderBy(F.desc("price")).limit(1)

    print(f"Total distinct customers: {distinct_customer_count}")
    print("Most Expensive Product:")
    expensive_product.show(truncate=False)


if __name__ == "__main__":
    run()
