package io.devorbit.sparklabs.batch.sales.rdd

object OrdersPerCity extends App {
  val customersById = RddUtils.customers.map(customer => (customer.customer_id, customer.city))
  val ordersByCustomer = RddUtils.orders.map(order => (order.customer_id, 1))

  customersById.join(ordersByCustomer)
    .map { case (_, (city, _)) => (city, 1) }
    .reduceByKey(_ + _)
    .sortBy(_._2, ascending = false)
    .collect()
    .foreach(println)
}
