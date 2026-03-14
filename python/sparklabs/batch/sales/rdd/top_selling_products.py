from sparklabs.batch.sales.rdd import utils


def run() -> None:
    rows = (
        utils.order_items.map(lambda item: (item["product_id"], 1))
        .reduceByKey(lambda left, right: left + right)
        .sortBy(lambda item: item[1], ascending=False)
        .collect()
    )
    for row in rows:
        print(row)


if __name__ == "__main__":
    run()
