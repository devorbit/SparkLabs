from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        SELECT oi.order_id,
               SUM(p.price * oi.quantity) AS total_revenue,
               COUNT(*) AS total_orders
        FROM order_items oi
        JOIN products p ON oi.product_id = p.product_id
        GROUP BY oi.order_id
        ORDER BY total_revenue DESC
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
