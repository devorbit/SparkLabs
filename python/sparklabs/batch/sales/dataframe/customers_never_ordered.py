from sparklabs.batch.sales.dataframe import utils


def run() -> None:
    utils.customers.join(utils.orders, ["customer_id"], "left_anti").show(truncate=False)


if __name__ == "__main__":
    run()
