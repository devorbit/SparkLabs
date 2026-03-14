package io.devorbit.sparklabs.batch.sales.sql

object ProductRank extends App {
  SqlUtils.sql(
    """
      |WITH category_counts AS (
      |  SELECT p.category, COUNT(*) AS category_count
      |  FROM (
      |    SELECT oi.product_id, COUNT(*) AS order_count
      |    FROM order_items oi
      |    GROUP BY oi.product_id
      |  ) product_orders
      |  JOIN products p ON product_orders.product_id = p.product_id
      |  GROUP BY p.category
      |)
      |SELECT category,
      |       category_count,
      |       DENSE_RANK() OVER (ORDER BY category_count DESC) AS category_rank
      |FROM category_counts
      |""".stripMargin
  ).show(false)
}
