package io.devorbit.sparklabs.batch.sales.sql

object AvgProductPrice extends App {
  SqlUtils.sql(
    """
      |SELECT category, ROUND(AVG(price)) AS avg_price
      |FROM products
      |GROUP BY category
      |ORDER BY avg_price DESC
      |""".stripMargin
  ).show(false)
}
