package io.devorbit.sparklabs.batch.sales.rdd

object DistinctCustomer extends App {
  private val distinctCustomerCount = RddUtils.orders.map(_.customer_id).count()
  private val expensiveProduct = RddUtils.products.takeOrdered(1)(Ordering.by(product => -product.price))

  println(s"Total distinct customers: $distinctCustomerCount")
  println("Most Expensive Product:")
  expensiveProduct.foreach(println)
}
