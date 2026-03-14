from sparklabs.batch.sales.rdd import utils


def run() -> None:
    distinct_customer_count = utils.orders.map(lambda order: order["customer_id"]).count()
    expensive_product = utils.products.takeOrdered(1, key=lambda product: -product["price"])

    print(f"Total distinct customers: {distinct_customer_count}")
    print("Most Expensive Product:")
    for row in expensive_product:
        print(row)


if __name__ == "__main__":
    run()
