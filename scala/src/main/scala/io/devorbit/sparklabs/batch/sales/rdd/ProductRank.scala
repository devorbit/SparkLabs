package io.devorbit.sparklabs.batch.sales.rdd

object ProductRank extends App {
  val categoryCounts = RddUtils.orderItems
    .map(item => (item.product_id, 1))
    .reduceByKey(_ + _)
    .map { case (productId, _) => (productId, 1) }
    .join(RddUtils.products.map(product => (product.product_id, product.category)))
    .map { case (_, (_, category)) => (category, 1) }
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .collect()

  categoryCounts.zipWithIndex.foreach { case ((category, count), index) =>
    println((category, count, index + 1))
  }
}
