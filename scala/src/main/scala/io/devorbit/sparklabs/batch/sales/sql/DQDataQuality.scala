package io.devorbit.sparklabs.batch.sales.sql

object DQDataQuality extends App {
  val duplicates = SqlUtils.sql(
    """
      |SELECT customer_id, COUNT(*) AS duplicate_count
      |FROM customers
      |GROUP BY customer_id
      |HAVING COUNT(*) > 1
      |""".stripMargin
  )

  if (duplicates.count() == 0) {
    println("PASS: customer_id is unique")
  } else {
    println("FAIL: duplicate customer_id values found")
    duplicates.show(false)
  }
}
