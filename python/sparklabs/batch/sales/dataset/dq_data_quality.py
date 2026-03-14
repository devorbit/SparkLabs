from sparklabs.batch.sales.dataset import utils
from sparklabs.batch.sales.dataset.models import Customer


def run() -> None:
    duplicates = utils.to_records(utils.customers, Customer)
    duplicate_count = (
        duplicates.map(lambda customer: (customer.customer_id, 1))
        .reduceByKey(lambda left, right: left + right)
        .filter(lambda pair: pair[1] > 1)
        .count()
    )
    if duplicate_count == 0:
        print("PASS: customer_id is unique")
    else:
        print(f"FAIL: found {duplicate_count} duplicate customer_id values")


if __name__ == "__main__":
    run()
