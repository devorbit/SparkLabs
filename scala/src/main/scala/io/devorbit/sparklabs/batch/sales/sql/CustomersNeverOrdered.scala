package io.devorbit.sparklabs.batch.sales.sql

object CustomersNeverOrdered extends App {
  SqlUtils.sql(
    """
      |SELECT c.*
      |FROM customers c
      |LEFT ANTI JOIN orders o ON c.customer_id = o.customer_id
      |""".stripMargin
  ).show(false)
}
