from sparklabs.batch.sales.rdd import utils


def run() -> None:
    duplicates = (
        utils.customers.map(lambda customer: (customer["customer_id"], 1))
        .reduceByKey(lambda left, right: left + right)
        .filter(lambda pair: pair[1] > 1)
    )
    duplicate_rows = duplicates.collect()
    if not duplicate_rows:
        print("PASS: customer_id is unique")
    else:
        for row in duplicate_rows:
            print(row)


if __name__ == "__main__":
    run()
