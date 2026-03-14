package io.devorbit.sparklabs.batch.sales.rdd

object TopSellingProducts extends App {
  RddUtils.orderItems
    .map(item => (item.product_id, 1))
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .collect()
    .foreach(println)
}
