from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT c.*
        FROM customers c
        LEFT ANTI JOIN orders o ON c.customer_id = o.customer_id
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
