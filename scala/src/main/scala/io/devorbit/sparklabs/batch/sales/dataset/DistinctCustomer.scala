package io.devorbit.sparklabs.batch.sales.dataset

import io.devorbit.sparklabs.batch.sales.dataframe.Utils

object DistinctCustomer extends App {
  import Utils.sparkSession.implicits._

  private val distinctCustomerCount = DatasetUtils.orders.map(_.customer_id).count()
  private val expensiveProduct = DatasetUtils.products.orderBy(org.apache.spark.sql.functions.desc("price")).limit(1)

  println(s"Total distinct customers: $distinctCustomerCount")
  println("Most Expensive Product:")
  expensiveProduct.show(false)
}
