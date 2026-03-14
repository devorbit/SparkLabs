package io.devorbit.sparklabs.batch.sales.rdd

object AvgProductPrice extends App {
  RddUtils.products
    .map(product => (product.category, (product.price, 1)))
    .reduceByKey { case ((sumA, countA), (sumB, countB)) => (sumA + sumB, countA + countB) }
    .map { case (category, (priceSum, count)) => (category, math.round(priceSum / count)) }
    .sortBy(_._2, ascending = false)
    .collect()
    .foreach(println)
}
