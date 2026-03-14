package io.devorbit.sparklabs.batch.sales.rdd

object DQDataQuality extends App {
  val duplicates = RddUtils.customers
    .map(customer => (customer.customer_id, 1))
    .reduceByKey(_ + _)
    .filter(_._2 > 1)

  if (duplicates.isEmpty()) println("PASS: customer_id is unique")
  else duplicates.collect().foreach(println)
}
