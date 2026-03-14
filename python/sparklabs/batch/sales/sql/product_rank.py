from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        WITH category_counts AS (
          SELECT p.category, COUNT(*) AS category_count
          FROM (
            SELECT oi.product_id, COUNT(*) AS order_count
            FROM order_items oi
            GROUP BY oi.product_id
          ) product_orders
          JOIN products p ON product_orders.product_id = p.product_id
          GROUP BY p.category
        )
        SELECT category,
               category_count,
               DENSE_RANK() OVER (ORDER BY category_count DESC) AS category_rank
        FROM category_counts
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
