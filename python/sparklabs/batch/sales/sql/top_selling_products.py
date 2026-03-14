from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT oi.product_id, COUNT(*) AS total_orders
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY oi.product_id
        ORDER BY total_orders DESC
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
