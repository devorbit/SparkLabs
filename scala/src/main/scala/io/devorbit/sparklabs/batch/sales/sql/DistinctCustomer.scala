package io.devorbit.sparklabs.batch.sales.sql

object DistinctCustomer extends App {
  private val customerCount = SqlUtils.sql(
    """
      |SELECT COUNT(customer_id) AS total_customers
      |FROM orders
      |""".stripMargin
  ).collect().head.getLong(0)

  private val expensiveProduct = SqlUtils.sql(
    """
      |SELECT *
      |FROM products
      |ORDER BY price DESC
      |LIMIT 1
      |""".stripMargin
  )

  println(s"Total distinct customers: $customerCount")
  println("Most Expensive Product:")
  expensiveProduct.show(false)
}
