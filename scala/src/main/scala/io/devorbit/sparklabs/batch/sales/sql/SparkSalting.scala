package io.devorbit.sparklabs.batch.sales.sql

object SparkSalting extends App {
  SqlUtils.sql(
    """
      |SELECT id, CAST(rand() * 10 AS INT) AS salt
      |FROM RANGE(20)
      |""".stripMargin
  ).show(false)
}
