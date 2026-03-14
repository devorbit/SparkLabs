package io.devorbit.sparklabs.batch.sales.rdd

object OrdersRevenueCaseWhen extends App {
  val ordersById = RddUtils.orders.map(order => (order.order_id, order))
  val productsById = RddUtils.products.map(product => (product.product_id, product))
  val promotionsByCategory = RddUtils.promotions.groupBy(_.category)

  RddUtils.orderItems
    .map(item => (item.order_id, item))
    .join(ordersById)
    .map { case (_, (item, order)) => (item.product_id, (item, order)) }
    .join(productsById)
    .map { case (_, ((item, order), product)) => (product.category, (item, order, product)) }
    .leftOuterJoin(promotionsByCategory)
    .map { case (_, ((item, order, product), promotionOpt)) =>
      val grossRevenue = product.price * item.quantity
      val discountPct = promotionOpt
        .flatMap(_.find(promotion => order.order_date >= promotion.start_date && order.order_date <= promotion.end_date))
        .map(_.discount_pct)
        .getOrElse(0.0)
      val discountValue = grossRevenue * discountPct / 100
      (item.order_id, (grossRevenue, discountValue, grossRevenue - discountValue))
    }
    .reduceByKey {
      case ((grossA, discountA, netA), (grossB, discountB, netB)) =>
        (grossA + grossB, discountA + discountB, netA + netB)
    }
    .sortBy(_._2._3, ascending = false)
    .collect()
    .foreach(println)
}
