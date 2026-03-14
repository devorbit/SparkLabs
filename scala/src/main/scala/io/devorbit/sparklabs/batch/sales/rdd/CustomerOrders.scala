package io.devorbit.sparklabs.batch.sales.rdd

object CustomerOrders extends App {
  val customerIds = RddUtils.customers.map(customer => (customer.customer_id, customer.customer_id))
  val orderIds = RddUtils.orders.map(order => (order.customer_id, order.customer_id))

  customerIds.join(orderIds)
    .map { case (customerId, _) => (customerId, 1L) }
    .reduceByKey((left, _) => left)
    .sortBy(_._2, ascending = false)
    .collect()
    .foreach(println)
}
