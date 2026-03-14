package io.devorbit.sparklabs.batch.sales.dataframe

import org.apache.spark.sql.functions._

object DistinctCustomer extends App {
  private val distinctCustomerCount = Utils.orders.select(col("customer_id")).count()
  private val expProduct  = Utils.products.orderBy(desc("price")).limit(1)

  println(s"Total distinct customers: $distinctCustomerCount")
  println(s"Most Expensive Product: $expProduct")
  expProduct.show(false )
//  Thread.sleep(100000000)

}
