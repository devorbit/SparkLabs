package io.devorbit.sparklabs.batch.sales.sql

object TopSellingProducts extends App {
  SqlUtils.sql(
    """
      |SELECT oi.product_id, COUNT(*) AS total_orders
      |FROM order_items oi
      |JOIN products p ON oi.product_id = p.product_id
      |GROUP BY oi.product_id
      |ORDER BY total_orders DESC
      |""".stripMargin
  ).show(false)
}
