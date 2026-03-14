from sparklabs.batch.sales.rdd import utils


def run() -> None:
    product_price_by_id = utils.products.map(
        lambda product: (product["product_id"], product["price"])
    )
    rows = (
        utils.order_items.map(lambda item: (item["product_id"], item))
        .join(product_price_by_id)
        .map(lambda item: (item[1][0]["order_id"], (item[1][1] * item[1][0]["quantity"], 1)))
        .reduceByKey(lambda left, right: (left[0] + right[0], left[1] + right[1]))
        .sortBy(lambda item: item[1][0], ascending=False)
        .collect()
    )
    for row in rows:
        print(row)


if __name__ == "__main__":
    run()
