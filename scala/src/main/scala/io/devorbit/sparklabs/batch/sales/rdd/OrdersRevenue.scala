package io.devorbit.sparklabs.batch.sales.rdd

object OrdersRevenue extends App {
  val productPriceById = RddUtils.products.map(product => (product.product_id, product.price))

  RddUtils.orderItems
    .map(item => (item.product_id, item))
    .join(productPriceById)
    .map { case (_, (item, price)) => (item.order_id, (price * item.quantity, 1)) }
    .reduceByKey { case ((revenueA, countA), (revenueB, countB)) => (revenueA + revenueB, countA + countB) }
    .sortBy(_._2._1, ascending = false)
    .collect()
    .foreach(println)
}
