from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT c.city, COUNT(*) AS total_orders_per_city
        FROM orders o
        JOIN customers c ON o.customer_id = c.customer_id
        GROUP BY c.city
        ORDER BY total_orders_per_city DESC
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
