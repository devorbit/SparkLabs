package io.devorbit.sparklabs.batch.sales.rdd

object CustomersNeverOrdered extends App {
  val orderedCustomers = RddUtils.orders.map(_.customer_id).distinct().collect().toSet
  RddUtils.customers
    .filter(customer => !orderedCustomers.contains(customer.customer_id))
    .collect()
    .foreach(println)
}
