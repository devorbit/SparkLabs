from sparklabs.batch.sales.rdd import utils


def run() -> None:
    rows = (
        utils.customers.map(lambda customer: (customer["customer_id"], customer["customer_id"]))
        .join(utils.orders.map(lambda order: (order["customer_id"], order["customer_id"])))
        .map(lambda item: (item[0], 1))
        .reduceByKey(lambda left, _: left)
        .sortBy(lambda item: item[1], ascending=False)
        .collect()
    )
    for row in rows:
        print(row)


if __name__ == "__main__":
    run()
