from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT c.customer_id, approx_count_distinct(c.customer_id) AS total_orders
        FROM customers c
        JOIN orders o ON c.customer_id = o.customer_id
        GROUP BY c.customer_id
        ORDER BY total_orders DESC
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
