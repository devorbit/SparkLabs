from sparklabs.batch.sales.sql import utils


def run() -> None:
    duplicates = utils.sql(
        """
        SELECT customer_id, COUNT(*) AS duplicate_count
        FROM customers
        GROUP BY customer_id
        HAVING COUNT(*) > 1
        """
    )
    if duplicates.count() == 0:
        print("PASS: customer_id is unique")
    else:
        print("FAIL: duplicate customer_id values found")
        duplicates.show(truncate=False)


if __name__ == "__main__":
    run()
