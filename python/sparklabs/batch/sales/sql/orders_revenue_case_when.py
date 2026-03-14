from sparklabs.batch.sales.sql import utils


def run() -> None:
    utils.sql(
        """
        WITH revenue_base AS (
          SELECT oi.order_id,
                 p.price * oi.quantity AS gross_revenue,
                 CASE
                   WHEN TO_DATE(o.order_date) BETWEEN TO_DATE(pr.start_date) AND TO_DATE(pr.end_date)
                     THEN COALESCE(pr.discount_pct, 0)
                   ELSE 0
                 END AS discount_pct
          FROM order_items oi
          JOIN orders o ON oi.order_id = o.order_id
          JOIN products p ON oi.product_id = p.product_id
          LEFT JOIN promotions pr ON p.category = pr.category
        )
        SELECT order_id,
               ROUND(SUM(gross_revenue), 2) AS gross_revenue,
               ROUND(SUM(gross_revenue * discount_pct / 100), 2) AS discount_value,
               ROUND(SUM(gross_revenue - (gross_revenue * discount_pct / 100)), 2) AS net_revenue
        FROM revenue_base
        GROUP BY order_id
        ORDER BY net_revenue DESC
        """
    ).show(truncate=False)


if __name__ == "__main__":
    run()
