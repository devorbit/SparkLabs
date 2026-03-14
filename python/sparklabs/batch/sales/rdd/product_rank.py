from sparklabs.batch.sales.rdd import utils


def run() -> None:
    rows = (
        utils.order_items.map(lambda item: (item["product_id"], 1))
        .reduceByKey(lambda left, right: left + right)
        .map(lambda item: (item[0], 1))
        .join(utils.products.map(lambda product: (product["product_id"], product["category"])))
        .map(lambda item: (item[1][1], 1))
        .reduceByKey(lambda left, right: left + right)
        .sortBy(lambda item: item[1], ascending=False)
        .collect()
    )
    for index, row in enumerate(rows, start=1):
        print((row[0], row[1], index))


if __name__ == "__main__":
    run()
