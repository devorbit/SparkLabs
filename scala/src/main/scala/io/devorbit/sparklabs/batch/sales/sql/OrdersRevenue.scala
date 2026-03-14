package io.devorbit.sparklabs.batch.sales.sql

object OrdersRevenue extends App {
  SqlUtils.sql(
    """
      |SELECT oi.order_id,
      |       SUM(p.price * oi.quantity) AS total_revenue,
      |       COUNT(*) AS total_orders
      |FROM order_items oi
      |JOIN products p ON oi.product_id = p.product_id
      |GROUP BY oi.order_id
      |ORDER BY total_revenue DESC
      |""".stripMargin
  ).show(false)
}
