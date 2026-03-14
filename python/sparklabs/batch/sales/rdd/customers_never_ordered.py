from sparklabs.batch.sales.rdd import utils


def run() -> None:
    ordered_customers = set(utils.orders.map(lambda order: order["customer_id"]).distinct().collect())
    for row in utils.customers.filter(
        lambda customer: customer["customer_id"] not in ordered_customers
    ).collect():
        print(row)


if __name__ == "__main__":
    run()
